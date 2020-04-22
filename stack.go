/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

package reftable

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

// CompactionStats holds some statistics of compaction over the
// lifetime of the stack.
type CompactionStats struct {
	Bytes    uint64
	Attempts int
	Failures int
}

// Stack is an auto-compacting stack of reftables.
type Stack struct {
	listFile    string
	reftableDir string
	cfg         Config

	// mutable
	stack              []*Reader
	merged             *Merged
	disableAutoCompact bool

	Stats CompactionStats
}

// NewStack returns a new stack.
func NewStack(dir string, cfg Config) (*Stack, error) {
	if cfg.HashID == NullHashID {
		cfg.HashID = SHA1ID
	}
	listFile := filepath.Join(dir, "tables.list")
	switch cfg.HashID {
	case SHA1ID, SHA256ID:
	default:
		return nil, fmt.Errorf("reftable: unknown hash ID %q", cfg.HashID)
	}

	st := &Stack{
		listFile:    listFile,
		reftableDir: dir,
		cfg:         cfg,
	}

	if err := st.reload(true); err != nil {
		return nil, err
	}

	return st, nil
}

func (st *Stack) String() string {
	var nms []string
	for _, r := range st.stack {
		nms = append(nms, r.Name())
	}
	return fmt.Sprintf("%v", nms)
}

func (st *Stack) readNames() ([]string, error) {
	c, err := ioutil.ReadFile(st.listFile)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	lines := bytes.Split(c, []byte("\n"))

	var res []string
	for _, l := range lines {
		if len(l) > 0 {
			res = append(res, string(l))
		}
	}

	return res, nil
}

// Returns the merged stack. The stack is only valid until the next
// write, as writes may trigger reloads
func (st *Stack) Merged() *Merged {
	return st.merged
}

// Close releases file descriptors associated with this stack.
func (st *Stack) Close() {
	for _, r := range st.stack {
		r.Close()
	}
	st.stack = nil
}

func (st *Stack) reloadOnce(names []string, reuseOpen bool) error {
	cur := map[string]*Reader{}

	for _, r := range st.stack {
		cur[r.Name()] = r
	}

	var newTables []*Reader
	defer func() {
		for _, t := range newTables {
			t.Close()
		}
	}()

	for _, name := range names {
		rd := cur[name]
		if reuseOpen && rd != nil {
			delete(cur, name)
		} else {
			bs, err := NewFileBlockSource(filepath.Join(st.reftableDir, name))
			if err != nil {
				return err
			}

			rd, err = NewReader(bs, name)
			if err != nil {
				return fmt.Errorf("NewReader(%s): %v", name, err)
			}
		}
		newTables = append(newTables, rd)
	}

	// success. Swap.
	st.stack = newTables
	for _, v := range cur {
		v.Close()
	}
	newTables = nil
	return nil
}

func (st *Stack) reload(reuseOpen bool) error {
	var delay time.Duration
	deadline := time.Now().Add(5 * time.Second / 2)
	for time.Now().Before(deadline) {
		names, err := st.readNames()
		if err != nil {
			return err
		}
		err = st.reloadOnce(names, reuseOpen)
		if err == nil {
			break
		}
		if !os.IsNotExist(err) {
			return err
		}
		after, err := st.readNames()
		if err != nil {
			return err
		}
		if reflect.DeepEqual(after, names) {
			// XXX: propogate name
			return os.ErrNotExist
		}

		// compaction changed name
		delay = time.Millisecond*time.Duration(1+rand.Intn(1)) + 2*delay
	}

	var tabs []*Reader
	for _, r := range st.stack {
		tabs = append(tabs, r)
	}

	m, err := NewMerged(tabs, st.cfg.HashID)
	if err != nil {
		return err
	}
	m.suppressDeletions = true
	st.merged = m
	return nil
}

// ErrLockFailure is returned for failed writes. On a failed write,
// the stack is reloaded, so the transaction may be retried.
var ErrLockFailure = errors.New("reftable: lock failure")

func (st *Stack) UpToDate() (bool, error) {
	names, err := st.readNames()
	if err != nil {
		return false, err
	}

	if len(names) != len(st.stack) {
		return false, nil
	}

	for i, e := range st.stack {
		if e.name != names[i] {
			return false, nil
		}
	}
	return true, nil
}

// Add a new reftable to stack, transactionally.
func (st *Stack) Add(write func(w *Writer) error) error {
	if err := st.add(write); err != nil {
		if err == ErrLockFailure {
			st.reload(true)
		}
		return err
	}

	if !st.disableAutoCompact {
		return st.AutoCompact()
	}
	return nil
}

func (st *Stack) add(write func(w *Writer) error) error {
	tr, err := st.NewAddition()
	if err != nil {
		return err
	}
	defer tr.Close()
	if err := tr.Add(write); err != nil {
		return err
	}

	return tr.Commit()
}

// Addition is a transaction that adds new tables to the top of the
// stack.
type Addition struct {
	lockFileName    string
	lockFile        *os.File
	stack           *Stack
	names           []string
	newTables       []string
	nextUpdateIndex uint64
}

// NewAddition returns an Addition instance. As a side effect, this
// takes a global filesystem lock on the ref database.
func (st *Stack) NewAddition() (*Addition, error) {
	tr := Addition{
		stack:        st,
		lockFileName: st.listFile + ".lock",
	}
	var err error
	tr.lockFile, err = os.OpenFile(tr.lockFileName, os.O_EXCL|os.O_CREATE|os.O_WRONLY, 0644)
	if os.IsExist(err) {
		return nil, ErrLockFailure
	}
	if err != nil {
		return nil, err
	}
	for _, e := range st.stack {
		tr.names = append(tr.names, e.name)
	}
	if ok, err := tr.stack.UpToDate(); err != nil {
		tr.Close()
		return nil, err
	} else if !ok {
		tr.Close()
		return nil, ErrLockFailure
	}
	tr.nextUpdateIndex = tr.stack.NextUpdateIndex()
	return &tr, nil
}

// Add calls the given function to write a new table at the top of
// the stack.
func (tr *Addition) Add(write func(w *Writer) error) error {
	fn := formatName(tr.nextUpdateIndex, tr.nextUpdateIndex)
	tab, err := ioutil.TempFile(tr.stack.reftableDir, fn+"-tmp-*.ref")
	if err != nil {
		return err
	}
	defer tab.Close()
	defer os.Remove(tab.Name())

	wr, err := NewWriter(tab, &tr.stack.cfg)
	if err != nil {
		return err
	}

	if err := write(wr); err != nil {
		return err
	}

	if err := wr.Close(); err != nil {
		if err == ErrEmptyTable {
			return nil
		}
		return err
	}

	if err := tab.Close(); err != nil {
		return err
	}

	if wr.minUpdateIndex < tr.nextUpdateIndex {
		return ErrLockFailure
	}
	dest := fn + ".ref"
	tr.names = append(tr.names, dest)
	tr.newTables = append(tr.newTables, dest)
	dest = filepath.Join(tr.stack.reftableDir, dest)
	if err := os.Rename(tab.Name(), dest); err != nil {
		return err
	}
	tr.nextUpdateIndex = wr.maxUpdateIndex + 1
	return nil
}

// Close releases all non-committed data from the transaction.
func (tr *Addition) Close() {
	for _, nm := range tr.newTables {
		os.Remove(filepath.Join(tr.stack.reftableDir, nm))
	}
	if tr.lockFile != nil {
		tr.lockFile.Close()
		tr.lockFile = nil
	}
	if tr.lockFileName != "" {
		os.Remove(tr.lockFileName)
		tr.lockFileName = ""
	}
}

// Commit commits the changes to the database, releasing the lock.
func (tr *Addition) Commit() error {
	if len(tr.newTables) == 0 {
		// Nothing to be done.
		return nil
	}

	if _, err := tr.lockFile.Write([]byte(strings.Join(tr.names, "\n"))); err != nil {
		tr.Close()
		return err
	}

	if err := tr.lockFile.Close(); err != nil {
		tr.Close()
		return err
	}
	tr.lockFile = nil
	if err := os.Rename(tr.lockFileName, tr.stack.listFile); err != nil {
		tr.Close()
		return err
	}
	tr.lockFileName = ""
	tr.newTables = nil

	return tr.stack.reload(true)
}

func formatName(min, max uint64) string {
	return fmt.Sprintf("0x%012x-0x%012x", min, max)
}

// NextUpdateIndex returns the update index at which to write the next table.
func (st *Stack) NextUpdateIndex() uint64 {
	if sz := len(st.stack); sz > 0 {
		return st.stack[sz-1].MaxUpdateIndex() + 1
	}
	return 1
}

// compactLocked writes the compacted version of tables [first,last]
// into a temporary file, whose name is returned.
func (st *Stack) compactLocked(first, last int, expiration *LogExpirationConfig) (string, error) {
	fn := formatName(st.stack[first].MinUpdateIndex(),
		st.stack[last].MaxUpdateIndex())

	tmpTable, err := ioutil.TempFile(st.reftableDir, fn+"_*.ref")
	if err != nil {
		return "", err
	}
	defer tmpTable.Close()
	rmName := tmpTable.Name()
	defer func() {
		if rmName != "" {
			os.Remove(rmName)
		}
	}()

	wr, err := NewWriter(tmpTable, &st.cfg)
	if err != nil {
		return "", err
	}

	if err := st.writeCompact(wr, first, last, expiration); err != nil {
		return "", err
	}

	if err := wr.Close(); err != nil {
		return "", err
	}

	if err := tmpTable.Close(); err != nil {
		return "", err
	}

	rmName = ""
	return tmpTable.Name(), nil
}

func (st *Stack) writeCompact(wr *Writer, first, last int, expiration *LogExpirationConfig) error {
	// do it.
	wr.SetLimits(st.stack[first].MinUpdateIndex(),
		st.stack[last].MaxUpdateIndex())

	var subtabs []*Reader
	for i := first; i <= last; i++ {
		subtabs = append(subtabs, st.stack[i])
	}

	merged, err := NewMerged(subtabs, st.cfg.HashID)
	if err != nil {
		return err
	}
	it, err := merged.SeekRef("")
	if err != nil {
		return err
	}

	for {
		var rec RefRecord
		ok, err := it.NextRef(&rec)
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		if first == 0 && rec.IsDeletion() {
			continue
		}

		if err := wr.AddRef(&rec); err != nil {
			return err
		}
	}

	it, err = merged.SeekLog("", math.MaxUint64)
	if err != nil {
		return err
	}
	for {
		var rec LogRecord
		ok, err := it.NextLog(&rec)
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		if expiration != nil {
			if expiration.Time > 0 && rec.Time < expiration.Time {
				continue
			}

			if expiration.MaxUpdateIndex != 0 && rec.UpdateIndex > expiration.MaxUpdateIndex {
				continue
			}
			if expiration.MinUpdateIndex != 0 && rec.UpdateIndex < expiration.MinUpdateIndex {
				continue
			}
		}

		if err := wr.AddLog(&rec); err != nil {
			return err
		}
	}
	return nil
}

func (st *Stack) compactRangeStats(first, last int, expiration *LogExpirationConfig) (bool, error) {
	ok, err := st.compactRange(first, last, expiration)
	if !ok {
		st.Stats.Failures++
	}
	return ok, err
}

func (st *Stack) compactRange(first, last int, expiration *LogExpirationConfig) (bool, error) {
	if first >= last && expiration == nil {
		return true, nil
	}
	st.Stats.Attempts++

	lockFileName := st.listFile + ".lock"
	lockFile, err := os.OpenFile(lockFileName, os.O_EXCL|os.O_CREATE|os.O_WRONLY, 0644)
	if os.IsExist(err) {
		return false, nil
	}

	lockFile.Close()
	defer func() {
		if lockFileName != "" {
			os.Remove(lockFileName)
		}
	}()

	if ok, err := st.UpToDate(); !ok || err != nil {
		return false, err
	}

	var deleteOnSuccess []string
	var subtableLocks []string
	defer func() {
		for _, l := range subtableLocks {
			os.Remove(l)
		}
	}()
	for i := first; i <= last; i++ {
		subtab := filepath.Join(st.reftableDir, st.stack[i].name)
		subtabLock := subtab + ".lock"
		l, err := os.OpenFile(subtabLock, os.O_EXCL|os.O_CREATE|os.O_WRONLY, 0644)

		if os.IsExist(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		l.Close()
		subtableLocks = append(subtableLocks, subtabLock)
		deleteOnSuccess = append(deleteOnSuccess, subtab)
	}

	if err := os.Remove(lockFileName); err != nil {
		return false, err
	}
	lockFileName = ""

	tmpTable, err := st.compactLocked(first, last, expiration)
	// Compaction + tombstones can create an empty table out of non-empty tables.
	emptyTable := (err == ErrEmptyTable)
	if emptyTable {
		err = nil
	}
	if err != nil {
		return false, err
	}

	lockFileName = st.listFile + ".lock"
	lockFile, err = os.OpenFile(lockFileName, os.O_EXCL|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return false, err
	}

	defer lockFile.Close()

	fn := formatName(
		st.stack[first].MinUpdateIndex(),
		st.stack[last].MaxUpdateIndex())

	fn += ".ref"
	destTable := filepath.Join(st.reftableDir, fn)

	if !emptyTable {
		if err := os.Rename(tmpTable, destTable); err != nil {
			return false, err
		}
	}

	var names []string
	for i := 0; i < first; i++ {
		names = append(names, st.stack[i].name)
	}

	if !emptyTable {
		names = append(names, fn)
	}

	for i := last + 1; i < len(st.stack); i++ {
		names = append(names, st.stack[i].name)
	}

	if _, err := lockFile.Write([]byte(strings.Join(names, "\n"))); err != nil {
		os.Remove(destTable)
		return false, err
	}

	if err := lockFile.Close(); err != nil {
		os.Remove(destTable)
	}

	if err := os.Rename(lockFileName, st.listFile); err != nil {
		os.Remove(destTable)
		return false, err
	}
	lockFileName = ""
	for _, nm := range deleteOnSuccess {
		if nm != destTable {
			// reflog expiry might cause us to reopen a
			// new file with the same name.
			os.Remove(nm)
		}
	}

	// If we expire log entries on a full compaction we write a
	// table with the same the (min,max) update index, but we have
	// to read from disk again.
	err = st.reload(expiration == nil)
	return true, err
}

func (st *Stack) tableSizesForCompaction() []uint64 {
	var res []uint64
	version := 1
	if st.cfg.HashID == SHA256ID {
		version = 2
	}
	var overhead = uint64(footerSize(version) + headerSize(version) - 1)
	for _, t := range st.stack {
		res = append(res, t.size-overhead)
	}
	return res
}

type segment struct {
	start int
	end   int // exclusive
	log   int
	bytes uint64
}

func (st *segment) size() int { return st.end - st.start }

func log2(sz uint64) int {
	base := uint64(2)
	if sz == 0 {
		panic("log(0)")
	}

	l := 0
	for sz > 0 {
		l++
		sz /= base
	}

	return l - 1
}

func sizesToSegments(sizes []uint64) []segment {
	var cur segment
	var res []segment
	for i, sz := range sizes {
		l := log2(sz)
		if cur.log != l && cur.bytes > 0 {
			res = append(res, cur)
			cur = segment{
				start: i,
			}
		}
		cur.log = l
		cur.end = i + 1
		cur.bytes += sz
	}

	res = append(res, cur)
	return res
}

func suggestCompactionSegment(sizes []uint64) *segment {
	segs := sizesToSegments(sizes)

	minSeg := segment{log: 64}
	for _, st := range segs {
		if st.size() == 1 {
			continue
		}

		if st.log < minSeg.log {
			minSeg = st
		}
	}
	if minSeg.size() == 0 {
		return nil
	}

	for minSeg.start > 0 {
		prev := minSeg.start - 1
		if log2(minSeg.bytes) < log2(sizes[prev]) {
			break
		}

		minSeg.start = prev
		minSeg.bytes += sizes[prev]
	}

	return &minSeg
}

// AutoCompact runs a compaction if the stack looks imbalanced.
func (st *Stack) AutoCompact() error {
	sizes := st.tableSizesForCompaction()
	seg := suggestCompactionSegment(sizes)
	if seg != nil {
		_, err := st.compactRangeStats(seg.start, seg.end-1, nil)
		return err
	}
	return nil
}

// CompactAll compacts the entire stack. If expiration is given, expire log entries.
func (st *Stack) CompactAll(expiration *LogExpirationConfig) error {
	_, err := st.compactRange(0, len(st.stack)-1, expiration)
	return err
}
