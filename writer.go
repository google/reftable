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
	opts  Options
	block []byte

	// The current block writer, or nil if it was just flushed.
	blockWriter *blockWriter
	index       []IndexRecord

	// hash => block offset positions.
	objIndex map[string][]uint64

	Stats Stats
}

func (opts *Options) setDefaults() {
	if opts.RestartInterval == 0 {
		opts.RestartInterval = 16
	}
	if opts.BlockSize == 0 {
		opts.BlockSize = 4096
	}
}

// NewWriter creates a writer.
func NewWriter(out io.Writer, opts *Options) (*Writer, error) {
	o := *opts
	o.setDefaults()
	w := &Writer{
		opts:  o,
		block: make([]byte, o.BlockSize),
	}

	if opts.BlockSize >= (1 << 24) {
		return nil, errors.New("reftable: invalid blocksize")
	}

	w.paddedWriter.out = out
	w.Stats.BlockStats = map[byte]*BlockStats{}
	for _, c := range "rgoi" {
		w.Stats.BlockStats[byte(c)] = new(BlockStats)
	}

	if !opts.SkipIndexObjects {
		w.objIndex = map[string][]uint64{}
	}

	w.blockWriter = w.newBlockWriter(BlockTypeRef)
	return w, nil
}

// newBlockWriter creates a new blockWriter
func (w *Writer) newBlockWriter(typ byte) *blockWriter {
	block := w.block

	var blockStart uint32
	if w.next == 0 {
		hb := w.headerBytes()
		blockStart = uint32(copy(block, hb))
	}

	bw := newBlockWriter(typ, block, blockStart)
	bw.restartInterval = w.opts.RestartInterval
	return bw
}

func (w *Writer) headerBytes() []byte {
	w.Stats.Header = Header{
		Magic:          magic,
		BlockSize:      w.opts.BlockSize,
		MinUpdateIndex: w.opts.MinUpdateIndex,
		MaxUpdateIndex: w.opts.MaxUpdateIndex,
	}
	h := w.Stats.Header
	h.BlockSize = h.BlockSize | (version << 24)
	buf := bytes.NewBuffer(make([]byte, 0, 24))
	binary.Write(buf, binary.BigEndian, h)
	return buf.Bytes()
}

// XXX indexHash
func (w *Writer) indexHash(hash []byte) {
	if w.opts.SkipIndexObjects {
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

// AddRef adds a RefRecord to the table. AddRef must be called in ascending order. AddRef cannot be called after AddLog is called.
func (w *Writer) AddRef(r *RefRecord) error {
	if r.UpdateIndex < w.opts.MinUpdateIndex || r.UpdateIndex > w.opts.MaxUpdateIndex {
		return fmt.Errorf("reftable: UpdateIndex %d outside bounds [%d, %d]",
			r.UpdateIndex, w.opts.MinUpdateIndex, w.opts.MaxUpdateIndex)
	}

	r.UpdateIndex -= w.opts.MinUpdateIndex

	if err := w.add(r); err != nil {
		return err
	}
	w.indexHash(r.Value)
	w.indexHash(r.TargetValue)
	return nil
}

// AddLog adds a LogRecord to the table. AddLog must be called in
// ascending order.
func (w *Writer) AddLog(l *LogRecord) error {
	if w.blockWriter != nil && w.blockWriter.getType() == BlockTypeRef {
		w.finishPublicSection()
	}

	w.next -= uint64(w.paddedWriter.pendingPadding)
	w.paddedWriter.pendingPadding = 0

	return w.add(l)
}

func (w *Writer) add(rec Record) error {
	k := rec.Key()
	if w.lastKey >= k {
		log.Panicf("keys must be ascending: got %q last %q", rec, w.lastRec)
	}
	w.lastKey = k
	w.lastRec = rec.String()

	if w.blockWriter == nil {
		w.blockWriter = w.newBlockWriter(rec.Type())
	}

	if t := w.blockWriter.getType(); t != rec.Type() {
		log.Panicf("add %c on block %c", rec.Type(), t)
	}
	if w.blockWriter.add(rec) {
		return nil
	}
	if err := w.flushBlock(); err != nil {
		return err
	}

	w.blockWriter = w.newBlockWriter(rec.Type())
	if !w.blockWriter.add(rec) {
		return fmt.Errorf("reftable: record %v too large for block size", rec)
	}
	return nil
}

// Close writes the footer and flushes the table to disk.
func (w *Writer) Close() error {
	w.finishPublicSection()
	hb := w.headerBytes()

	buf := bytes.NewBuffer(hb)
	w.Stats.Footer = Footer{
		RefIndexOffset: w.Stats.BlockStats[BlockTypeRef].IndexOffset,
		ObjOffset:      w.Stats.BlockStats[BlockTypeObj].Offset,
		ObjIndexOffset: w.Stats.BlockStats[BlockTypeObj].IndexOffset,
		LogOffset:      w.Stats.BlockStats[BlockTypeLog].Offset,
		LogIndexOffset: w.Stats.BlockStats[BlockTypeLog].IndexOffset,
	}

	f := w.Stats.Footer
	f.ObjOffset = f.ObjOffset<<5 | uint64(w.Stats.ObjectIDLen)

	if err := binary.Write(buf, binary.BigEndian, &f); err != nil {
		return err
	}

	h := crc32.NewIEEE()
	h.Write(buf.Bytes())
	crc := h.Sum32()

	binary.Write(buf, binary.BigEndian, crc)

	w.paddedWriter.pendingPadding = 0
	n, err := w.paddedWriter.Write(buf.Bytes(), 0)
	if n != footerSize {
		log.Panicf("footer size %d", n)
	}
	return err
}

const debug = false

func (w *Writer) flushBlock() error {
	if w.blockWriter == nil {
		return nil
	}
	if w.blockWriter.entries == 0 {
		return nil
	}
	typ := w.blockWriter.getType()
	blockStats := w.Stats.BlockStats[typ]
	// blockStats.Offset maybe 0 legitimately, so look at
	// blockStats.Blocks instead
	if blockStats.Blocks == 0 {
		// Record where the first block of a type starts.
		blockStats.Offset = w.next
	}
	raw := w.blockWriter.finish()
	padding := int(w.opts.BlockSize) - len(raw)
	if w.opts.Unpadded || typ == BlockTypeLog {
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
	w.index = append(w.index, IndexRecord{
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

	if typ == BlockTypeRef && !w.opts.SkipIndexObjects {
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

	w.blockWriter = w.newBlockWriter(BlockTypeObj)
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

		w.blockWriter = w.newBlockWriter(BlockTypeObj)
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
	w.lastKey = ""
	typ := w.blockWriter.getType()
	if err := w.flushBlock(); err != nil {
		return err
	}

	var indexStart uint64
	maxLevel := 0

	threshold := 3
	if w.opts.Unpadded {
		// always write index for unaligned files.
		threshold = 1
	}
	before := w.Stats.BlockStats[BlockTypeIndex].Blocks
	for len(w.index) > threshold {
		maxLevel++
		indexStart = w.next
		w.blockWriter = w.newBlockWriter(BlockTypeIndex)
		idx := w.index
		w.index = nil
		for _, i := range idx {
			if w.blockWriter.add(&i) {
				continue
			}

			if err := w.flushBlock(); err != nil {
				return err
			}
			w.blockWriter = w.newBlockWriter(BlockTypeIndex)
			if !w.blockWriter.add(&i) {
				panic("fail on fresh block")
			}
		}
	}
	w.index = nil
	if err := w.flushBlock(); err != nil {
		return err
	}

	blockStats := w.Stats.BlockStats[typ]
	blockStats.IndexBlocks = w.Stats.BlockStats[BlockTypeIndex].Blocks - before
	blockStats.IndexOffset = indexStart
	blockStats.MaxIndexLevel = maxLevel
	return nil
}

// XXX all varint size checks should also check max field size.
