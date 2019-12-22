// Copyright 2019 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package reftable

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
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
	stack  []*Reader
	merged *Merged

	Stats CompactionStats
}

// NewStack returns a new stack.
func NewStack(dir, listFile string, cfg Config) (*Stack, error) {
	st := &Stack{
		listFile:    listFile,
		reftableDir: dir,
		cfg:         cfg,
	}

	if err := st.reload(); err != nil {
		return nil, err
	}

	return st, nil
}

func (s *Stack) readNames() ([]string, error) {
	c, err := ioutil.ReadFile(s.listFile)
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
func (s *Stack) Merged() *Merged {
	return s.merged
}

// Close releases file descriptors associated with this stack.
func (s *Stack) Close() {
	for _, r := range s.stack {
		r.Close()
	}
	s.stack = nil
}

func (s *Stack) reloadOnce(names []string) error {
	cur := map[string]*Reader{}
	for _, r := range s.stack {
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
		if rd != nil {
			delete(cur, name)
		} else {
			bs, err := NewFileBlockSource(filepath.Join(s.reftableDir, name))
			if err != nil {
				return err
			}

			rd, err = NewReader(bs, name)
			if err != nil {
				return err
			}
		}
		newTables = append(newTables, rd)
	}

	// success. Swap.
	s.stack = newTables
	for _, v := range cur {
		v.Close()
	}
	newTables = nil
	return nil
}

func (s *Stack) reload() error {
	var delay time.Duration
	deadline := time.Now().Add(5 * time.Second / 2)
	for time.Now().Before(deadline) {
		names, err := s.readNames()
		if err != nil {
			return err
		}
		err = s.reloadOnce(names)
		if err == nil {
			break
		}
		if !os.IsNotExist(err) {
			return err
		}
		after, err := s.readNames()
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
	for _, r := range s.stack {
		tabs = append(tabs, r)
	}

	m, err := NewMerged(tabs)
	if err != nil {
		return err
	}
	s.merged = m
	return nil
}

// ErrLockFailure is returned for failed writes. On a failed write,
// the stack is reloaded, so the transaction may be retried.
var ErrLockFailure = errors.New("reftable: lock failure")

func (s *Stack) UpToDate() (bool, error) {
	names, err := s.readNames()
	if err != nil {
		return false, err
	}

	if len(names) != len(s.stack) {
		return false, nil
	}

	for i, e := range s.stack {
		if e.name != names[i] {
			return false, nil
		}
	}
	return true, nil
}

// Add a new reftable to stack, transactionally.
func (s *Stack) Add(write func(w *Writer) error) error {
	if err := s.add(write); err != nil {
		if err == ErrLockFailure {
			s.reload()
		}
		return err
	}

	return s.AutoCompact()
}

func (s *Stack) add(write func(w *Writer) error) error {
	lockFile := s.listFile + ".lock"
	f, err := os.OpenFile(lockFile, os.O_EXCL|os.O_CREATE|os.O_WRONLY, 0644)
	if os.IsExist(err) {
		return ErrLockFailure
	}
	if err != nil {
		return err
	}

	defer f.Close()
	defer func() {
		if lockFile != "" {
			os.Remove(lockFile)
		}
	}()

	if ok, err := s.UpToDate(); err != nil {
		return err
	} else if !ok {
		return ErrLockFailure
	}

	for _, e := range s.stack {
		fmt.Fprintf(f, "%s\n", e.name)
	}

	next := s.NextUpdateIndex()
	fn := formatName(next, next)
	tab, err := ioutil.TempFile(s.reftableDir, fn+"*.ref")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())

	wr, err := NewWriter(tab, &s.cfg)
	if err != nil {
		return err
	}

	if err := write(wr); err != nil {
		return err
	}

	if err := wr.Close(); err != nil {
		return err
	}

	if err := tab.Close(); err != nil {
		return err
	}

	if wr.minUpdateIndex < next {
		return ErrLockFailure
	}

	dest := fn + ".ref"
	fmt.Fprintf(f, "%s\n", dest)
	dest = filepath.Join(s.reftableDir, dest)
	if err := os.Rename(tab.Name(), dest); err != nil {
		// XXX LockFailure?
		return err
	}

	if err := f.Close(); err != nil {
		os.Remove(dest)
		return err
	}

	if err := os.Rename(lockFile, s.listFile); err != nil {
		os.Remove(dest)
		return err
	}
	lockFile = ""

	return s.reload()
}

func formatName(min, max uint64) string {
	return fmt.Sprintf("%012x-%012x", min, max)
}

// NextUpdateIndex returns the update index at which to write the next table.
func (s *Stack) NextUpdateIndex() uint64 {
	if sz := len(s.stack); sz > 0 {
		return s.stack[sz-1].MaxUpdateIndex() + 1
	}
	return 1
}

// compactLocked writes the compacted version of tables [first,last]
// into a temporary file, whose name is returned.
func (s *Stack) compactLocked(first, last int) (string, error) {
	fn := formatName(s.stack[first].MinUpdateIndex(),
		s.stack[last].MaxUpdateIndex())

	tmpTable, err := ioutil.TempFile(s.reftableDir, fn+"_*.ref")
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

	wr, err := NewWriter(tmpTable, &s.cfg)
	if err != nil {
		return "", err
	}

	if err := s.writeCompact(wr, first, last); err != nil {
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

func (s *Stack) writeCompact(wr *Writer, first, last int) error {
	// do it.
	wr.SetLimits(s.stack[first].MinUpdateIndex(),
		s.stack[last].MaxUpdateIndex())

	var subtabs []*Reader
	for i := first; i <= last; i++ {
		subtabs = append(subtabs, s.stack[i])
	}

	merged, err := NewMerged(subtabs)
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

		if first == 0 && rec.isDeletion() {
			continue
		}

		if err := wr.AddRef(&rec); err != nil {
			return err
		}
	}

	it, err = merged.SeekLog("", 0xffffffffffffffff)
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

		if err := wr.AddLog(&rec); err != nil {
			return err
		}
	}
	return nil
}

func (s *Stack) compactRangeStats(first, last int) (bool, error) {
	ok, err := s.compactRange(first, last)
	if !ok {
		s.Stats.Failures++
	}
	return ok, err
}

func (s *Stack) compactRange(first, last int) (bool, error) {
	if first >= last {
		return true, nil
	}
	s.Stats.Attempts++

	lockFileName := s.listFile + ".lock"
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

	if ok, err := s.UpToDate(); !ok || err != nil {
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
		subtab := filepath.Join(s.reftableDir, s.stack[i].name)
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

	tmpTable, err := s.compactLocked(first, last)
	if err != nil {
		return false, err
	}

	lockFileName = s.listFile + ".lock"
	lockFile, err = os.OpenFile(lockFileName, os.O_EXCL|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return false, err
	}

	defer lockFile.Close()

	fn := formatName(
		s.stack[first].MinUpdateIndex(),
		s.stack[last].MaxUpdateIndex())

	fn += ".ref"
	destTable := filepath.Join(s.reftableDir, fn)

	if err := os.Rename(tmpTable, destTable); err != nil {
		return false, err
	}

	var buf bytes.Buffer
	for i := 0; i < first; i++ {
		fmt.Fprintf(&buf, "%s\n", s.stack[i].name)
	}
	fmt.Fprintf(&buf, "%s\n", fn)
	for i := last + 1; i < len(s.stack); i++ {
		fmt.Fprintf(&buf, "%s\n", s.stack[i].name)
	}

	if _, err := lockFile.Write(buf.Bytes()); err != nil {
		os.Remove(destTable)
		return false, err
	}

	if err := lockFile.Close(); err != nil {
		os.Remove(destTable)
	}

	if err := os.Rename(lockFileName, s.listFile); err != nil {
		os.Remove(destTable)
		return false, err
	}

	lockFileName = ""
	for _, nm := range deleteOnSuccess {
		os.Remove(nm)
	}

	err = s.reload()
	return true, err
}

func (s *Stack) tableSizesForCompaction() []uint64 {
	var res []uint64
	for _, t := range s.stack {
		// overhead is 92 bytes
		res = append(res, t.size-91)
	}
	return res
}

type segment struct {
	start int
	end   int // exclusive
	log   int
	bytes uint64
}

func (s *segment) size() int { return s.end - s.start }

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

func segments(sizes []uint64) []segment {
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
	segs := segments(sizes)

	var minIdx = -1
	var minLog = 64
	for i, s := range segs {
		if s.size() == 1 {
			continue
		}

		if s.log < minLog {
			minIdx = i
			minLog = s.log
		}
	}

	if minIdx == -1 {
		return nil
	}

	minSeg := &segs[minIdx]
	for minSeg.start > 0 {
		prev := minSeg.start - 1
		if log2(minSeg.bytes) < log2(sizes[prev]) {
			break
		}

		minSeg.start = prev
		minSeg.bytes += sizes[prev]
	}

	return minSeg
}

// AutoCompact runs a compaction if the stack looks imbalanced.
func (s *Stack) AutoCompact() error {
	sizes := s.tableSizesForCompaction()
	seg := suggestCompactionSegment(sizes)
	if seg != nil {
		_, err := s.compactRangeStats(seg.start, seg.end-1)
		return err
	}
	return nil
}

// CompactAll compacts the entire stack.
func (s *Stack) CompactAll() error {
	_, err := s.compactRange(0, len(s.stack)-1)
	return err
}
