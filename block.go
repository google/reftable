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
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

func isBlockType(typ byte) bool {
	switch typ {
	case 'g', 'i', 'r', 'o':
		return true
	}
	return false
}

// blockWriter writes a single block.
type blockWriter struct {
	// immutable
	buf             []byte
	blockSize       uint32
	headerOff       uint32
	restartInterval int

	// mutable
	next     uint32
	restarts []uint32
	lastKey  string
	entries  int
}

// newBlockWriter creates a writer for the given block type.
func newBlockWriter(typ byte, buf []byte, headerOff uint32) *blockWriter {
	bw := &blockWriter{
		buf:       buf,
		headerOff: headerOff,
		blockSize: uint32(len(buf)),
	}

	bw.buf[headerOff] = typ
	bw.next = headerOff + 4
	// Some sensible default.
	bw.restartInterval = 16

	return bw
}

func (w *blockWriter) getType() byte {
	return w.buf[w.headerOff]
}

// add adds a record, returning true, or if it does not fit, false.
func (w *blockWriter) add(r record) bool {
	last := w.lastKey
	if w.entries%w.restartInterval == 0 {
		last = ""
	}

	buf := w.buf[w.next:]
	start := buf
	n, restart, ok := encodeKey(buf, last, r.Key(), r.valType())
	if !ok {
		return false
	}
	buf = buf[n:]

	n, ok = r.encode(buf)
	if !ok {
		return false
	}
	buf = buf[n:]

	return w.registerRestart(len(start)-len(buf), restart, r.Key())
}

func (w *blockWriter) registerRestart(n int, restart bool, key string) bool {
	rlen := len(w.restarts)
	if rlen >= maxRestarts {
		restart = false
	}

	if restart {
		rlen++
	}
	if 2+3*rlen+n > len(w.buf[w.next:]) {
		return false
	}
	if restart {
		w.restarts = append(w.restarts, w.next)
	}
	w.next += uint32(n)
	w.lastKey = key
	w.entries++
	return true
}

func putU24(out []byte, i uint32) {
	out[0] = byte((i >> 16) & 0xff)
	out[1] = byte((i >> 8) & 0xff)
	out[2] = byte((i) & 0xff)
}

func getU24(in []byte) uint32 {
	return uint32(in[0])<<16 | uint32(in[1])<<8 | uint32(in[2])
}

// finish finalizes the block, and returns the unpadded block.
func (w *blockWriter) finish() (data []byte) {
	for _, r := range w.restarts {
		putU24(w.buf[w.next:], r)
		w.next += 3
	}
	binary.BigEndian.PutUint16(w.buf[w.next:], uint16(len(w.restarts)))
	w.next += 2
	putU24(w.buf[w.headerOff+1:], w.next)

	data = w.buf[:w.next]

	if w.getType() == blockTypeLog {
		compressed := bytes.Buffer{}
		compressed.Write(data[:w.headerOff+4])

		zw, _ := zlib.NewWriterLevel(&compressed, 9)
		_, err := zw.Write(data[w.headerOff+4:])
		if err != nil {
			panic("in mem zlib write")
		}
		if err := zw.Close(); err != nil {
			panic("in mem zlib close")
		}
		c := compressed.Bytes()
		return c
	}

	return data
}

// blockReader holds data for reading a block. It is immutable, so it
// is safe for concurrent access.
type blockReader struct {
	// The offset of the block header, 24 for the first block
	headerOff uint32

	// block is the data, including header, file header, but
	// excluding restarts and padding.
	block []byte

	// The bytes holding the restart offsets
	restartBytes []byte

	// Size of the (compressed) block, including everything. This
	// indicates where the next block will be
	fullBlockSize uint32

	restartCount uint16
}

func (br *blockReader) getType() byte {
	return br.block[br.headerOff]
}

// newBlockWriter prepares for reading a block.
func newBlockReader(block []byte, headerOff uint32, tableBlockSize uint32) (*blockReader, error) {
	fullBlockSize := tableBlockSize
	typ := block[headerOff]
	if !isBlockType(typ) {
		return nil, fmt.Errorf("reftable: unknown block type %c", typ)
	}

	sz := getU24(block[headerOff+1:])

	if typ == blockTypeLog {
		decompress := make([]byte, 0, sz)
		buf := bytes.NewBuffer(block)
		out := bytes.NewBuffer(decompress)

		before := buf.Len()

		// Consume header
		io.CopyN(out, buf, int64(headerOff+4))
		r, err := zlib.NewReader(buf)
		if err != nil {
			return nil, err
		}
		// Have to use io.Copy. zlib stream has a terminator,
		// which we must consume, so go until EOF.
		if _, err := io.Copy(out, r); err != nil {
			return nil, err
		}

		r.Close()

		block = out.Bytes()
		fullBlockSize = uint32(before - buf.Len())
	} else if fullBlockSize == 0 {
		// unaligned table.
		fullBlockSize = sz
	}
	block = block[:sz]

	restartCount := binary.BigEndian.Uint16(block[len(block)-2:])
	restartStart := len(block) - 2 - 3*int(restartCount)
	restartBytes := block[restartStart:]
	block = block[:restartStart]

	br := &blockReader{
		block:         block,
		fullBlockSize: fullBlockSize,
		headerOff:     headerOff,
		restartCount:  restartCount,
		restartBytes:  restartBytes,
	}

	return br, nil
}

// restart returns the offset within the block of the i-th key
// restart.
func (br *blockReader) restartOffset(i int) uint32 {
	return getU24(br.restartBytes[3*i:])
}

// blockIter iterates over the block. A blockIter is a value type, so
// it can be copied.
type blockIter struct {
	br *blockReader

	lastKey    string
	nextOffset uint32
}

// seek positions the iter to just before the given key.
func (bi *blockIter) seek(key string) error {
	seeked, err := bi.br.seek(key)
	if err != nil {
		return err
	}
	*bi = *seeked
	return nil
}

// start returns an iterator positioned at the start of the block.
func (br *blockReader) start(bi *blockIter) {
	*bi = blockIter{
		br:         br,
		nextOffset: uint32(br.headerOff + 4),
	}
}

// seek returns an iterator positioned just before the given key
func (br *blockReader) seek(key string) (*blockIter, error) {
	var decodeErr error

	// Find the first restart key beyond the wanted key.
	j := sort.Search(int(br.restartCount),
		func(i int) bool {
			rkey, err := decodeRestartKey(br.block, br.restartOffset(i))
			if err != nil {
				decodeErr = err
			}
			return key < rkey
		})

	if decodeErr != nil {
		return nil, decodeErr
	}
	it := &blockIter{
		br: br,
	}

	if j > 0 {
		// We have a restart beyond the key, go one back to be before the wanted key
		j--
		it.nextOffset = br.restartOffset(j)
	} else {
		it.nextOffset = br.headerOff + 4
	}

	rec := newRecord(br.getType(), "")
	for {
		next := *it

		ok, err := next.Next(rec)
		if err != nil {
			return nil, err
		}

		if !ok || rec.Key() >= key {
			return it, nil
		}
		*it = next
	}
}

// Next implement the Iterator interface.
func (bi *blockIter) Next(r record) (bool, error) {
	if bi.nextOffset >= uint32(len(bi.br.block)) {
		return false, nil
	}

	buf := bi.br.block[bi.nextOffset:]
	start := buf
	n, key, valType, ok := decodeKey(buf, bi.lastKey)
	if !ok {
		return false, fmtError
	}
	buf = buf[n:]

	if n, ok := r.decode(buf, key, valType); !ok {
		return false, fmtError
	} else {
		buf = buf[n:]
	}

	bi.lastKey = r.Key()
	bi.nextOffset += uint32(len(start) - len(buf))
	return true, nil
}
