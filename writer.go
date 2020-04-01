/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

package reftable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"sort"
)

type paddedWriter struct {
	out io.Writer

	// pendingPadding is how much padding to write before next
	// block
	pendingPadding int
}

func (w *paddedWriter) Write(b []byte, padding int) (int, error) {
	if w.pendingPadding > 0 {
		pad := make([]byte, w.pendingPadding)
		_, err := w.out.Write(pad)
		if err != nil {
			return 0, err
		}
		w.pendingPadding = 0
	}
	w.pendingPadding = padding
	n, err := w.out.Write(b)
	n += padding
	return n, err
}

// Writer writes a single reftable.
type Writer struct {
	paddedWriter paddedWriter

	lastKey string
	lastRec string

	// offset where to write next block.
	next uint64

	// write options
	cfg   Config
	block []byte

	minUpdateIndex uint64
	maxUpdateIndex uint64

	// The current block writer, or nil if it was just flushed.
	blockWriter *blockWriter
	index       []indexRecord

	// hash => block offset positions.
	objIndex map[string][]uint64

	Stats Stats

	header header
	footer footer
}

func (cfg *Config) setDefaults() {
	if cfg.RestartInterval == 0 {
		cfg.RestartInterval = 16
	}
	if cfg.BlockSize == 0 {
		cfg.BlockSize = defaultBlockSize
	}
}

// NewWriter creates a writer.
func NewWriter(out io.Writer, cfg *Config) (*Writer, error) {
	o := *cfg
	o.setDefaults()
	w := &Writer{
		cfg:   o,
		block: make([]byte, o.BlockSize),
	}

	if cfg.BlockSize >= (1 << 24) {
		return nil, errors.New("reftable: invalid blocksize")
	}

	w.paddedWriter.out = out
	if !cfg.SkipIndexObjects {
		w.objIndex = map[string][]uint64{}
	}

	w.blockWriter = w.newBlockWriter(blockTypeRef)

	return w, nil
}

func (w *Writer) headerSize() int {
	switch w.cfg.HashID {
	case NullHashID, SHA1ID:
		return headerSize(1)
	case SHA256ID:
		return headerSize(2)
	default:
		panic("hash")
	}
}

func (w *Writer) footerSize() int {
	switch w.cfg.HashID {
	case NullHashID, SHA1ID:
		return 68
	case SHA256ID:
		return 72
	default:
		panic("hash")
	}
}

// newBlockWriter creates a new blockWriter
func (w *Writer) newBlockWriter(typ byte) *blockWriter {
	block := w.block

	var blockStart uint32
	if w.next == 0 {
		blockStart = uint32(w.headerSize())
	}

	bw := newBlockWriter(typ, block, blockStart, w.cfg.HashID.Size())
	bw.restartInterval = w.cfg.RestartInterval
	return bw
}

func (w *Writer) headerBytes() []byte {
	h := header{
		Magic:          magic,
		BlockSize:      w.cfg.BlockSize,
		MinUpdateIndex: w.minUpdateIndex,
		MaxUpdateIndex: w.maxUpdateIndex,
		HashID:         w.cfg.HashID,
	}

	v := uint32(1)
	if w.cfg.HashID == SHA256ID {
		v = 2
	}
	h.BlockSize = h.BlockSize | (v << 24)
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, h)
	return buf.Bytes()[:headerSize(int(v))]
}

func (w *Writer) indexHash(hash []byte) {
	if w.cfg.SkipIndexObjects {
		return
	}
	off := w.next
	if hash == nil {
		return
	}
	str := string(hash)
	l := w.objIndex[str]
	if len(l) > 0 && l[len(l)-1] == off {
		return
	}

	w.objIndex[str] = append(l, off)
}

// SetLimits sets the range of the records to be written. It should be
// called before calling AddRef or AddLog
func (w *Writer) SetLimits(min, max uint64) {
	w.minUpdateIndex = min
	w.maxUpdateIndex = max
}

// AddRef adds a RefRecord to the table. AddRef must be called in ascending order. AddRef cannot be called after AddLog is called.
func (w *Writer) AddRef(r *RefRecord) error {
	if r.RefName == "" {
		return fmt.Errorf("reftable: must specify RefName")
	}

	if r.UpdateIndex < w.minUpdateIndex || r.UpdateIndex > w.maxUpdateIndex {
		return fmt.Errorf("reftable: UpdateIndex %d outside bounds [%d, %d]",
			r.UpdateIndex, w.minUpdateIndex, w.maxUpdateIndex)
	}

	cpy := *r
	cpy.UpdateIndex -= w.minUpdateIndex
	if err := w.add(&cpy); err != nil {
		return err
	}
	w.indexHash(r.Value)
	w.indexHash(r.TargetValue)
	return nil
}

// AddLog adds a LogRecord to the table. AddLog must be called in
// ascending order.
func (w *Writer) AddLog(l *LogRecord) error {
	if l.RefName == "" {
		return fmt.Errorf("reftable: must specify RefName")
	}

	if w.blockWriter != nil && w.blockWriter.getType() == blockTypeRef {
		if err := w.finishPublicSection(); err != nil {
			return nil
		}
	}

	w.next -= uint64(w.paddedWriter.pendingPadding)
	w.paddedWriter.pendingPadding = 0

	return w.add(l)
}

func (w *Writer) add(rec record) error {
	k := rec.key()
	if w.lastKey >= k {
		log.Panicf("keys must be ascending: got %q last %q", rec, w.lastRec)
	}
	w.lastKey = k
	w.lastRec = rec.String()

	if w.blockWriter == nil {
		w.blockWriter = w.newBlockWriter(rec.typ())
	}

	if t := w.blockWriter.getType(); t != rec.typ() {
		log.Panicf("add %c on block %c", rec.typ(), t)
	}
	if w.blockWriter.add(rec) {
		return nil
	}
	if err := w.flushBlock(); err != nil {
		return err
	}

	w.blockWriter = w.newBlockWriter(rec.typ())
	if !w.blockWriter.add(rec) {
		return fmt.Errorf("reftable: record %v too large for block size", rec)
	}
	return nil
}

// Close writes the footer and flushes the table to disk.
func (w *Writer) Close() error {
	if err := w.finishPublicSection(); err != nil {
		return err
	}
	w.paddedWriter.pendingPadding = 0

	hb := w.headerBytes()
	emptyTable := w.next == 0

	if emptyTable {
		// Even an empty file needs a file header, separate
		// from the file footer.
		if _, err := w.paddedWriter.Write(hb, 0); err != nil {
			return err
		}
	}

	buf := bytes.NewBuffer(hb)
	f := footer{
		RefIndexOffset: w.Stats.RefStats.IndexOffset,
		ObjOffset:      w.Stats.ObjStats.Offset,
		ObjIndexOffset: w.Stats.ObjStats.IndexOffset,
		LogOffset:      w.Stats.LogStats.Offset,
		LogIndexOffset: w.Stats.LogStats.IndexOffset,
	}

	f.ObjOffset = f.ObjOffset<<5 | uint64(w.Stats.ObjectIDLen)

	if err := binary.Write(buf, binary.BigEndian, &f); err != nil {
		return err
	}

	h := crc32.NewIEEE()
	h.Write(buf.Bytes())
	crc := h.Sum32()

	binary.Write(buf, binary.BigEndian, crc)

	if _, err := w.paddedWriter.Write(buf.Bytes(), 0); err != nil {
		return err
	}

	if emptyTable {
		return ErrEmptyTable
	}

	return nil
}

const debug = false

func (w *Writer) getBlockStats(typ byte) *BlockStats {
	switch typ {
	case blockTypeRef:
		return &w.Stats.RefStats
	case blockTypeLog:
		return &w.Stats.LogStats
	case blockTypeObj:
		return &w.Stats.ObjStats
	case blockTypeIndex:
		return &w.Stats.idxStats
	}

	panic(typ)
}

func (w *Writer) flushBlock() error {
	if w.blockWriter == nil {
		return nil
	}
	if w.blockWriter.entries == 0 {
		return nil
	}
	typ := w.blockWriter.getType()
	blockStats := w.getBlockStats(typ)
	// blockStats.Offset maybe 0 legitimately, so look at
	// blockStats.Blocks instead
	if blockStats.Blocks == 0 {
		// Record where the first block of a type starts.
		blockStats.Offset = w.next
	}
	raw := w.blockWriter.finish()
	if w.next == 0 {
		copy(raw, w.headerBytes())
	}

	padding := int(w.cfg.BlockSize) - len(raw)
	if w.cfg.Unaligned || typ == blockTypeLog {
		padding = 0
	}

	blockStats.Entries += w.blockWriter.entries
	blockStats.Restarts += len(w.blockWriter.restarts)
	blockStats.Blocks++
	w.Stats.Blocks++

	if debug {
		log.Printf("block %c off %d sz %d (%d)",
			w.blockWriter.getType(), w.next, len(raw), getU24(raw[w.blockWriter.headerOff+1:]))
	}
	n, err := w.paddedWriter.Write(raw, padding)
	if err != nil {
		return err
	}
	w.index = append(w.index, indexRecord{
		w.blockWriter.lastKey,
		w.next,
	})
	w.next += uint64(n)
	w.blockWriter = nil
	return nil
}

func (w *Writer) finishPublicSection() error {
	if w.blockWriter == nil {
		return nil
	}

	typ := w.blockWriter.getType()
	if err := w.finishSection(); err != nil {
		return err
	}

	if typ == blockTypeRef && !w.cfg.SkipIndexObjects && w.Stats.RefStats.IndexBlocks > 0 {
		if err := w.dumpObjectIndex(); err != nil {
			return err
		}
	}

	w.blockWriter = nil
	return nil
}

func commonPrefixSize(a, b string) int {
	p := 0
	for p < len(a) && p < len(b) {
		if a[p] != b[p] {
			break
		}
		p++
	}
	return p
}

func (w *Writer) dumpObjectIndex() error {
	strs := make([]string, 0, len(w.objIndex))
	for k := range w.objIndex {
		strs = append(strs, k)
	}
	sort.Strings(strs)

	last := ""
	maxCommon := 0

	strs = uniq(strs)

	last = ""
	for _, k := range strs {
		c := commonPrefixSize(last, k)
		if c > maxCommon {
			maxCommon = c
		}
		last = k
	}
	w.Stats.ObjectIDLen = maxCommon + 1

	w.blockWriter = w.newBlockWriter(blockTypeObj)
	for _, k := range strs {
		offsets := w.objIndex[k]
		k = k[:w.Stats.ObjectIDLen]
		rec := &objRecord{[]byte(k), offsets}

		if w.blockWriter.add(rec) {
			continue
		}

		if err := w.flushBlock(); err != nil {
			return err
		}

		w.blockWriter = w.newBlockWriter(blockTypeObj)
		if !w.blockWriter.add(rec) {
			rec.Offsets = nil
			if !w.blockWriter.add(rec) {
				panic("truncated obj record does not fit in fresh block")
			}
		}
	}

	return w.finishSection()
}

func (w *Writer) finishSection() error {
	typ := w.blockWriter.getType()
	if err := w.flushBlock(); err != nil {
		return err
	}

	var indexStart uint64
	maxLevel := 0

	threshold := 3
	if w.cfg.Unaligned {
		// always write index for unaligned files.
		threshold = 1
	}
	before := w.Stats.idxStats.Blocks
	for len(w.index) > threshold {
		maxLevel++
		indexStart = w.next
		w.blockWriter = w.newBlockWriter(blockTypeIndex)
		idx := w.index
		w.index = nil
		for _, i := range idx {
			if w.blockWriter.add(&i) {
				continue
			}

			if err := w.flushBlock(); err != nil {
				return err
			}
			w.blockWriter = w.newBlockWriter(blockTypeIndex)
			if !w.blockWriter.add(&i) {
				panic("fail on fresh block")
			}
		}
	}
	w.index = nil
	if err := w.flushBlock(); err != nil {
		return err
	}

	blockStats := w.getBlockStats(typ)
	blockStats.IndexBlocks = w.Stats.idxStats.Blocks - before
	blockStats.IndexOffset = indexStart
	blockStats.MaxIndexLevel = maxLevel

	// Reinit lastKey, as the next section can start with any key.
	w.lastKey = ""
	return nil
}

// XXX all varint size checks should also check max field size.
