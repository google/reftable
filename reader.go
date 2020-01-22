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
	"fmt"
	"hash/crc32"
	"log"
)

// ByteBlockSource is an in-memory block source.
type ByteBlockSource struct {
	Source []byte
}

func (s *ByteBlockSource) Size() uint64 {
	return uint64(len(s.Source))
}
func (s *ByteBlockSource) ReadBlock(off uint64, sz int) ([]byte, error) {
	return s.Source[off : off+uint64(sz)], nil
}

func (s *ByteBlockSource) Close() error {
	return nil
}

// readerOffsets has metadata for each block type
type readerOffsets struct {
	// Present is true if the section is present in the file.
	Present bool

	// Offset is where to find the first block of this type.
	Offset uint64

	// Offset of the index, or 0 if not present
	IndexOffset uint64
}

// Reader allows reading from a reftable.
type Reader struct {
	header header
	footer footer

	objectIDLen int

	name string
	src  BlockSource
	size uint64

	offsets map[byte]readerOffsets
}

func (r *Reader) Close() {
	r.src.Close()
}

func (r *Reader) Name() string {
	return r.name
}

func (r *Reader) getBlock(off uint64, sz uint32) ([]byte, error) {
	if off >= r.size {
		return nil, nil
	}

	if off+uint64(sz) > r.size {
		sz = uint32(r.size - off)
	}

	return r.src.ReadBlock(off, int(sz))
}

// NewReader creates a reader for a reftable file.
func NewReader(src BlockSource, name string) (*Reader, error) {
	r := &Reader{
		size: src.Size() - footerSize,
		src:  src,
		name: name,
	}
	headblock, err := src.ReadBlock(0, headerSize)
	if err != nil {
		return nil, err
	}

	footblock, err := src.ReadBlock(r.size, footerSize)
	if err != nil {
		return nil, err
	}

	if 0 != bytes.Compare(headblock, footblock[:headerSize]) {
		return nil, fmt.Errorf("reftable: start header %q != tail header %q",
			headblock, footblock[:headerSize])
	}

	footBuf := bytes.NewBuffer(footblock)
	if err := binary.Read(footBuf, binary.BigEndian, &r.header); err != nil {
		return nil, err
	}

	if err := binary.Read(footBuf, binary.BigEndian, &r.footer); err != nil {
		return nil, err
	}

	var gotCRC32 uint32
	if err := binary.Read(footBuf, binary.BigEndian, &gotCRC32); err != nil {
		return nil, err
	}

	if r.header.Magic != magic {
		return nil, fmt.Errorf("reftable: got magic %q, want %q", r.header.Magic, magic)
	}

	readVersion := r.header.BlockSize >> 24
	if readVersion != version {
		return nil, fmt.Errorf("reftable: no support for format version %d", readVersion)
	}
	r.header.BlockSize &= (1 << 24) - 1

	if footBuf.Len() > 0 {
		log.Panicf("footer size %d", footBuf.Len())
	}

	// This is a deficiency of the format: we should be able
	// to tell from the footer if where the refs start
	headBlock, err := src.ReadBlock(0, headerSize+1)
	if err != nil {
		return nil, err
	}

	r.objectIDLen = int(r.footer.ObjOffset & ((1 << 5) - 1))
	r.footer.ObjOffset >>= 5

	wantCRC32 := crc32.ChecksumIEEE(footblock[:footerSize-4])
	if gotCRC32 != wantCRC32 {
		return nil, fmt.Errorf("reftable: got CRC %x, want CRC %x", gotCRC32, wantCRC32)
	}

	firstBlockTyp := headBlock[headerSize]
	r.offsets = map[byte]readerOffsets{
		blockTypeRef: {
			Present:     firstBlockTyp == blockTypeRef,
			Offset:      0,
			IndexOffset: r.footer.RefIndexOffset,
		},
		blockTypeLog: {
			Present:     firstBlockTyp == blockTypeLog || r.footer.LogOffset > 0,
			Offset:      r.footer.LogOffset,
			IndexOffset: r.footer.LogIndexOffset,
		},
		blockTypeObj: {
			Present:     r.footer.ObjOffset > 0,
			Offset:      r.footer.ObjOffset,
			IndexOffset: r.footer.ObjIndexOffset,
		},
	}

	// In case of blocksize==0, should read the entire thing
	// into a ByteBlockSource?

	return r, nil
}

// tableIter iterates over a section in the file. It is a value type,
// which can be copied with copyFrom.
type tableIter struct {
	r        *Reader
	typ      byte
	blockOff uint64
	bi       blockIter
	finished bool
}

// nextInBlock advances the block iterator, or returns false we are
// past the last record.
func (i *tableIter) nextInBlock(rec record) (bool, error) {
	ok, err := i.bi.Next(rec)
	if ok {
		r, isRef := rec.(*RefRecord)
		if isRef {
			r.UpdateIndex += i.r.header.MinUpdateIndex
		}
	}
	if err != nil {
		err = fmt.Errorf("block %c, off %d: %v", i.typ, i.blockOff, err)
	}
	return ok, err
}

// Next implements the Iterator interface
func (i *tableIter) Next(rec record) (bool, error) {
	for {
		if i.finished {
			return false, nil
		}

		ok, err := i.nextInBlock(rec)
		if err != nil || ok {
			return ok, err
		}
		ok, err = i.nextBlock()
		if err != nil {
			return false, err
		}
		if !ok {
			return ok, err
		}
	}
}

// extractBlockSize returns the block size from the block header
func extractBlockSize(block []byte, off uint64) (typ byte, size uint32, err error) {
	if off == 0 {
		block = block[24:]
	}

	if !isBlockType(block[0]) {
		return 0, 0, fmtError
	}

	return block[0], getU24(block[1:]), nil
}

// newBlockReader opens a block of the given type, starting at
// nextOff. It is not an error to read beyond the end of file, or
// specify an offset into a different type of block. If this happens,
// a nil blockReader is returned.
func (r *Reader) newBlockReader(nextOff uint64, wantTyp byte) (br *blockReader, err error) {
	if nextOff >= r.size {
		return
	}

	guessBlockSize := r.header.BlockSize
	if guessBlockSize == 0 {
		guessBlockSize = defaultBlockSize
	}
	block, err := r.getBlock(nextOff, guessBlockSize)
	if err != nil {
		return nil, err
	}

	blockTyp, blockSize, err := extractBlockSize(block, nextOff)
	if err != nil {
		return nil, err
	}

	if wantTyp != blockTypeAny && blockTyp != wantTyp {
		return nil, nil
	}

	if blockSize > guessBlockSize {
		block, err = r.getBlock(nextOff, blockSize)
		if err != nil {
			return nil, err
		}
	}

	var headerOff uint32
	if nextOff == 0 {
		headerOff = headerSize
	}

	return newBlockReader(block, headerOff, r.header.BlockSize)
}

// nextBlock moves to the next block, or returns false fi there is none.
func (i *tableIter) nextBlock() (bool, error) {
	// XXX this is wrong. If the 'r' block is a followed by a 'g'
	// block, this will read into random uncompressed data.
	nextBlockOff := i.blockOff + uint64(i.bi.br.fullBlockSize)
	br, err := i.r.newBlockReader(nextBlockOff, i.typ)
	if err != nil {
		return false, fmt.Errorf("reftable: reading %c block at 0x%x: %v", i.typ, nextBlockOff, err)
	}
	if br == nil {
		i.finished = true
		return false, nil
	}
	br.start(&i.bi)
	i.blockOff = nextBlockOff
	return true, nil
}

// start returns an iterator positioned at the start of the given
// block type. If index is specified, it returns an iterator at the
// start of the top-level index for that block type.
func (r *Reader) start(typ byte, index bool) (*tableIter, error) {
	off := r.offsets[typ].Offset
	if index {
		off = r.offsets[typ].IndexOffset
		typ = blockTypeIndex
		if off == 0 {
			return nil, nil
		}
	}
	return r.tabIterAt(off, typ)
}

// tabIterAt returns a tableIter for the data at given offset.
func (r *Reader) tabIterAt(off uint64, wantTyp byte) (*tableIter, error) {
	br, err := r.newBlockReader(off, wantTyp)
	if err != nil || br == nil {
		return nil, err
	}

	ti := &tableIter{
		r:        r,
		typ:      br.getType(),
		blockOff: off,
	}
	br.start(&ti.bi)
	return ti, nil
}

// seekRecord returns an iterator pointed to just before the key specified
// by the record
func (r *Reader) seekRecord(rec record) (iterator, error) {
	if !r.offsets[rec.typ()].Present {
		return &emptyIterator{}, nil
	}
	return r.seek(rec)
}

func (r *Reader) SeekRef(name string) (*Iterator, error) {
	ref := RefRecord{
		RefName: name,
	}
	impl, err := r.seekRecord(&ref)
	if err != nil {
		return nil, err
	}
	return &Iterator{impl}, nil
}

func (r *Reader) SeekLog(name string, updateIndex uint64) (*Iterator, error) {
	log := LogRecord{
		RefName:     name,
		UpdateIndex: updateIndex,
	}

	impl, err := r.seekRecord(&log)
	if err != nil {
		return nil, err
	}
	return &Iterator{impl}, nil
}

// seek seekes to the key specified by the record
func (r *Reader) seek(rec record) (*tableIter, error) {
	typ := rec.typ()
	if rec.key() == newRecord(rec.typ(), "").key() {
		return r.start(typ, false)
	}

	idx := r.offsets[typ].IndexOffset
	if idx > 0 {
		return r.seekIndexed(rec)
	}

	tabIter, err := r.start(rec.typ(), false)
	if err != nil {
		return nil, err
	}

	ok, err := r.seekLinear(tabIter, rec)
	if ok {
		return tabIter, nil
	}

	return nil, err
}

// seekIndexed seeks to the `want` record, using its index.
func (r *Reader) seekIndexed(want record) (*tableIter, error) {
	idxIter, err := r.start(want.typ(), true)
	if err != nil {
		return nil, err
	}

	wantIdx := &indexRecord{
		LastKey: want.key(),
	}

	ok, err := r.seekLinear(idxIter, wantIdx)
	if err != nil || !ok {
		return nil, err
	}

	for {
		var rec indexRecord
		ok, err := idxIter.Next(&rec)
		if !ok {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		tabIter, err := r.tabIterAt(rec.Offset, blockTypeAny)
		if err != nil {
			return nil, err
		}

		err = tabIter.bi.seek(want.key())
		if err != nil {
			return nil, err
		}

		if tabIter.typ == want.typ() {
			return tabIter, nil
		}

		if tabIter.typ != blockTypeIndex {
			log.Panicf("got type %c following indexes", tabIter.typ)
		}

		idxIter = tabIter
	}
}

// seekLinear iterates tabIter to just before the wanted record.
func (r *Reader) seekLinear(tabIter *tableIter, want record) (bool, error) {
	rec := newRecord(want.typ(), "")

	wantKey := want.key()
	// skip blocks
	var last tableIter
	for {
		last = *tabIter

		ok, err := tabIter.nextBlock()
		if err != nil {
			return false, err
		}

		if !ok {
			break
		}

		ok, err = tabIter.Next(rec)
		if err != nil {
			return false, err
		}
		if !ok {
			panic("read from fresh block failed")
		}
		if rec.key() > wantKey {
			break
		}
	}

	// within the block, skip the right key
	*tabIter = last
	var err error
	err = tabIter.bi.seek(wantKey)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (r *Reader) MaxUpdateIndex() uint64 {
	return r.header.MaxUpdateIndex
}

func (r *Reader) MinUpdateIndex() uint64 {
	return r.header.MinUpdateIndex
}

// indexedTableRefIter iterates over a refs, returning refs pointing
// to a given object ID. The ref blocks to consider must be specified
// upfront.
type indexedTableRefIter struct {
	r   *Reader
	oid []byte

	// mutable

	// block offsets of remaining refblocks to look into
	offsets  []uint64
	cur      blockIter
	finished bool
}

func (i *indexedTableRefIter) nextBlock() error {
	if len(i.offsets) == 0 {
		i.finished = true
		return nil
	}
	nextOff := i.offsets[0]
	i.offsets = i.offsets[1:]

	br, err := i.r.newBlockReader(nextOff, blockTypeRef)
	if err != nil {
		return err
	}
	if br == nil {
		return fmt.Errorf("reftable: indexed block does not exist")
	}

	br.start(&i.cur)
	return nil
}

// Next implements the Iterator interface
func (i *indexedTableRefIter) Next(rec record) (bool, error) {
	ref := rec.(*RefRecord)
	for {
		ok, err := i.cur.Next(ref)
		if err != nil {
			return false, err
		}
		if !ok {
			if err := i.nextBlock(); err != nil {
				return false, err
			}
			if i.finished {
				return false, nil
			}
			// XXX test for this case
			continue
		}

		if bytes.Compare(ref.Value, i.oid) == 0 || bytes.Compare(ref.TargetValue, i.oid) == 0 {
			return true, nil
		}
	}
}

// RefsFor iterates over refs that point to `oid`.
func (r *Reader) RefsFor(oid []byte) (*Iterator, error) {
	if r.offsets[blockTypeObj].Present {
		return r.refsForIndexed(oid)
	}

	it, err := r.start(blockTypeRef, false)
	if err != nil {
		return nil, err
	}
	return &Iterator{&filteringRefIterator{
		tab:         r,
		oid:         oid,
		doubleCheck: false,
		it:          it,
	}}, nil
}

func (r *Reader) refsForIndexed(oid []byte) (*Iterator, error) {
	want := &objRecord{HashPrefix: oid[:r.objectIDLen]}

	it, err := r.seek(want)
	if err != nil {
		return nil, err
	}

	got := objRecord{}
	ok, err := it.Next(&got)
	if err != nil {
		return nil, err
	}
	if !ok || got.key() != want.key() {
		return &Iterator{&emptyIterator{}}, nil
	}

	tr := &indexedTableRefIter{
		r:       r,
		oid:     oid,
		offsets: got.Offsets,
	}
	if err := tr.nextBlock(); err != nil {
		return nil, err
	}
	return &Iterator{tr}, nil
}
