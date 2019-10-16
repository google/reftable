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
	"encoding/binary"
	"fmt"
	"sort"
)

type blockWriter2 struct {
	// immutable
	buf             []byte
	scratch         []byte
	blockSize       uint32
	restartInterval int

	// mutable
	next     uint32
	restarts []uint32
	lastKey  string
	entries  int
}

func newBlockWriter2(block []byte) *blockWriter2 {
	bw := &blockWriter2{
		buf:       block,
		scratch:   make([]byte, len(block)),
		blockSize: uint32(len(block)),
	}

	// Some sensible default.
	bw.restartInterval = 16
	bw.next = 4
	return bw
}

func (w *blockWriter2) add(rec Record) bool {
	n, ok := rec.encode(w.scratch)
	if !ok {
		return false
	}
	return w.addKeyVal(string(rec.Type())+rec.Key(), w.scratch[:n], rec.valType())
}

func (w *blockWriter2) addKeyVal(key string, val []byte, extra uint8) bool {
	last := w.lastKey
	if w.entries%w.restartInterval == 0 {
		last = ""
	}

	buf := w.buf[w.next:]
	start := buf
	n, restart, ok := encodeKey(buf, last, key, extra)
	if !ok {
		return false
	}
	buf = buf[n:]

	if s, ok := putVarInt(buf, uint64(len(val))); !ok {
		return false
	} else {
		buf = buf[s:]
	}

	if len(buf) < len(val) {
		return false
	}

	copy(buf, val)
	buf = buf[len(val):]

	return w.registerAdd(len(start)-len(buf), restart, key)
}

func (w *blockWriter2) registerAdd(n int, restart bool, key string) bool {
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

// finish finalizes the block, and returns the unpadded block.
func (w *blockWriter2) finish() (data []byte) {
	for _, r := range w.restarts {
		putU24(w.buf[w.next:], r)
		w.next += 3
	}
	binary.BigEndian.PutUint16(w.buf[w.next:], uint16(len(w.restarts)))
	w.next += 2
	putU24(w.buf[1:], w.next)

	data = w.buf[:w.next]

	return data
}

// blockReader holds immutable data for reading a block.
type blockReader2 struct {
	// block is the data, including header, file header, but
	// excluding restarts and padding.
	block []byte

	fullBlockSize uint32
	// The bytes holding the restart offsets
	restartBytes []byte

	restartCount uint16
}

func newBlockReader2(block []byte) *blockReader2 {
	sz := getU24(block[1:])
	block = block[:sz]
	restartCount := binary.BigEndian.Uint16(block[len(block)-2:])
	restartStart := len(block) - 2 - 3*int(restartCount)
	restartBytes := block[restartStart:]
	block = block[:restartStart]

	br := &blockReader2{
		block:        block,
		restartCount: restartCount,
		restartBytes: restartBytes,
	}

	return br
}

func (br *blockReader2) restart(i int) uint32 {
	return getU24(br.restartBytes[3*i:])
}

// blockIters are value types
type blockIter2 struct {
	br *blockReader2

	lastKey    string
	nextOffset uint32
}

func (bi *blockIter2) seek(key string) error {
	seeked, err := bi.br.seek(key)
	if err != nil {
		return err
	}
	*bi = *seeked
	return nil
}

func (br *blockReader2) start() *blockIter2 {
	return &blockIter2{
		br:         br,
		nextOffset: uint32(4),
	}
}

func (br *blockReader2) seek(wanted string) (*blockIter2, error) {
	var decodeErr error
	j := sort.Search(int(br.restartCount),
		func(i int) bool {
			rkey, err := decodeRestartKey(br.block, br.restart(i))
			if err != nil {
				decodeErr = err
			}
			return wanted < rkey
		})

	if decodeErr != nil {
		return nil, decodeErr
	}
	it := &blockIter2{
		br: br,
	}

	if j > 0 {
		j--
		it.nextOffset = br.restart(j)
	} else {
		it.nextOffset = 4
	}

	for {
		next := *it

		ok, key, _, _, err := next.next()
		if err != nil {
			return nil, err
		}

		if !ok || key >= wanted {
			return it, nil
		}
		*it = next
	}
}

func (bi *blockIter2) next() (ok bool, key string, val []byte, valType uint8, err error) {
	err = fmtError
	if bi.nextOffset >= uint32(len(bi.br.block)) {
		err = nil
		return
	}

	var n int
	buf := bi.br.block[bi.nextOffset:]
	start := buf
	n, key, valType, ok = decodeKey(buf, bi.lastKey)
	if !ok {
		return
	}
	buf = buf[n:]

	sz, n := getVarInt(buf)
	if n <= 0 {
		return
	}
	buf = buf[n:]

	if len(buf) < int(sz) {
		return
	}

	val = buf[:sz]
	buf = buf[sz:]

	bi.lastKey = key
	bi.nextOffset += uint32(len(start) - len(buf))
	ok = true
	err = nil
	return
}

func (br *blockReader2) Seek(rec Record) (*blockIter2, error) {
	return br.seek(string(rec.Type()) + rec.Key())
}

func (bi *blockIter2) Next(rec Record) (ok bool, err error) {
	ok, key, val, valType, err := bi.next()
	if !ok || err != nil {
		return ok, err
	}

	typ := key[0]
	key = key[1:]
	if typ != rec.Type() {
		return false, fmt.Errorf("reftable: wrong type")
	}
	n, ok := rec.decode(val, key, valType)
	if !ok {
		return false, fmtError
	}
	if n != len(val) {
		return false, fmt.Errorf("reftable: trailing junk")
	}
	return true, nil
}
