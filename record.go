/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

package reftable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

func newRecord(typ byte, key string) record {
	switch typ {
	case blockTypeLog:
		l := &LogRecord{}
		if key != "" {
			if !l.decodeKey(key) {
				panic("LogRecord key should be well formed")
			}
		}
		return l
	case blockTypeRef:
		return &RefRecord{
			RefName: key,
		}
	case blockTypeObj:
		return &objRecord{
			HashPrefix: []byte(key),
		}
	case blockTypeIndex:
		return &indexRecord{LastKey: key}
	}
	return nil
}

type objRecord struct {
	HashPrefix []byte
	Offsets    []uint64
}

func getVarInt(buf []byte) (uint64, int) {
	if len(buf) == 0 {
		return 0, -1
	}
	ptr := 0
	val := uint64(buf[ptr] & 0x7f)
	for buf[ptr]&0x80 != 0 {
		ptr++
		if ptr > len(buf) {
			return 0, -1
		}
		val = ((val + 1) << 7) | uint64(buf[ptr]&0x7f)
	}

	return val, ptr + 1
}

func putVarInt(buf []byte, val uint64) (n int, ok bool) {
	var dest [10]byte

	i := 9
	dest[i] = byte(val & 0x7f)
	i--
	for {
		val >>= 7
		if val == 0 {
			break
		}

		val--
		dest[i] = 0x80 | byte(val&0x7f)
		i--
	}

	n = len(dest[i+1:])
	if n > len(buf) {
		return 0, false
	}
	copy(buf, dest[i+1:])
	return n, true
}

func encodeKey(buf []byte, prevKey, key string, extra uint8) (n int, restart bool, fits bool) {
	start := buf
	prefixLen := commonPrefixSize(prevKey, key)
	restart = prefixLen == 0

	s, ok := putVarInt(buf, uint64(prefixLen))
	if !ok {
		return
	}
	buf = buf[s:]

	suffixLen := len(key) - prefixLen
	if s, ok := putVarInt(buf, uint64(suffixLen<<3)|uint64(extra)); !ok {
		return
	} else {
		buf = buf[s:]
	}
	if len(buf) < suffixLen {
		return
	}
	copy(buf, key[prefixLen:])
	buf = buf[suffixLen:]

	n = len(start) - len(buf)
	return n, restart, true
}

func (r *RefRecord) typ() byte {
	return blockTypeRef
}

func (r *RefRecord) key() string {
	return r.RefName
}

func (r *RefRecord) copyFrom(in record) {
	*r = *in.(*RefRecord)
}

func (r *RefRecord) isDeletion() bool {
	return r.Value == nil && r.TargetValue == nil && r.Target == ""
}

func (r *RefRecord) String() string {
	return fmt.Sprintf("ref(%s)", r.RefName)
}

func (r *RefRecord) decode(buf []byte, key string, valType uint8, hashSize int) (n int, ok bool) {
	*r = RefRecord{}
	start := buf
	r.RefName = key
	delta, s := getVarInt(buf)
	if s <= 0 {
		return
	}
	r.UpdateIndex = delta
	buf = buf[s:]

	switch valType {
	case 1, 2:
		if len(buf) < hashSize {
			return
		}
		r.Value = make([]byte, hashSize)
		copy(r.Value, buf[:hashSize])
		buf = buf[hashSize:]
		if valType == 1 {
			break
		}
		if len(buf) < hashSize {
			return
		}
		r.TargetValue = make([]byte, hashSize)
		copy(r.TargetValue, buf[:hashSize])
		buf = buf[hashSize:]
	case 3:
		tsize, s := getVarInt(buf)
		if s <= 0 {
			return
		}
		buf = buf[s:]
		if len(buf) < int(tsize) {
			return
		}

		r.Target = string(buf[:tsize])
		buf = buf[tsize:]
	}

	return len(start) - len(buf), true
}

func (r *RefRecord) valType() uint8 {
	var valueType uint8
	if len(r.Value) > 0 {
		if len(r.TargetValue) > 0 {
			valueType = 2
		} else {
			valueType = 1
		}
	} else if len(r.Target) > 0 {
		valueType = 3
	}
	return valueType
}

func (r *RefRecord) encode(buf []byte, hashSize int) (n int, fits bool) {
	start := buf

	s, ok := putVarInt(buf, uint64(r.UpdateIndex))
	if !ok {
		return
	}
	buf = buf[s:]

	if s := len(r.Value); s > 0 {
		if len(buf) < s {
			return
		}
		copy(buf, r.Value)
		buf = buf[s:]
	}
	if s := len(r.TargetValue); s > 0 {
		if len(buf) < s {
			return
		}
		copy(buf, r.TargetValue)
		buf = buf[s:]
	}
	if len(r.Target) > 0 {
		s, ok := putVarInt(buf, uint64(len(r.Target)))
		if !ok {
			return
		}
		buf = buf[s:]
		if len(buf) < len(r.Target) {
			return
		}
		copy(buf, r.Target)
		buf = buf[len(r.Target):]
	}
	return len(start) - len(buf), true
}

func (r *objRecord) key() string {
	return string(r.HashPrefix)
}

func (r *objRecord) copyFrom(in record) {
	*r = *in.(*objRecord)
}

func (r *objRecord) typ() byte {
	return blockTypeObj
}

func (r *objRecord) String() string {
	return fmt.Sprintf("obj(%x)", r.HashPrefix)
}

func (r *objRecord) valType() uint8 {
	var lower uint8
	if l := len(r.Offsets); l > 0 && l < 8 {
		lower = uint8(l)
	}

	return lower
}

func (r *objRecord) encode(buf []byte, hashSize int) (n int, fits bool) {
	start := buf

	if len(r.Offsets) == 0 || len(r.Offsets) >= 8 {
		s, ok := putVarInt(buf, uint64(len(r.Offsets)))
		if !ok {
			return
		}
		buf = buf[s:]
	}

	if len(r.Offsets) == 0 {
		return len(start) - len(buf), true
	}
	s, ok := putVarInt(buf, uint64(r.Offsets[0]))
	if !ok {
		return
	}
	buf = buf[s:]

	last := r.Offsets[0]
	for _, o := range r.Offsets[1:] {
		s, ok := putVarInt(buf, o-last)
		if !ok {
			return
		}
		buf = buf[s:]
		last = o
	}

	return len(start) - len(buf), true
}

func (r *objRecord) decode(buf []byte, prefix string, cnt3 uint8, hashSize int) (n int, ok bool) {
	*r = objRecord{}

	start := buf
	r.HashPrefix = []byte(prefix)
	var count uint64
	if cnt3 == 0 {
		count, n = getVarInt(buf)
		if n <= 0 {
			return
		}
		buf = buf[n:]
	} else {
		count = uint64(cnt3)
	}

	if count == 0 {
		return len(start) - len(buf), true
	}

	r.Offsets = make([]uint64, 1, count)
	r.Offsets[0], n = getVarInt(buf)
	if n <= 0 {
		return
	}
	buf = buf[n:]
	count--

	last := r.Offsets[0]
	for count > 0 {
		o, n := getVarInt(buf)
		if n <= 0 {
			return 0, false
		}
		buf = buf[n:]
		count--

		o += last
		r.Offsets = append(r.Offsets, o)
		last = o
	}
	return len(start) - len(buf), true
}

type indexRecord struct {
	LastKey string
	Offset  uint64
}

func (r *indexRecord) key() string {
	return r.LastKey
}

func (r *indexRecord) typ() byte {
	return blockTypeIndex
}

func (r *indexRecord) valType() byte {
	return 0
}

func (r *indexRecord) copyFrom(in record) {
	*r = *in.(*indexRecord)
}

func (r *indexRecord) String() string {
	return fmt.Sprintf("idx(%s)", r.LastKey)
}

func (r *indexRecord) decode(buf []byte, key string, valType uint8, hashSize int) (n int, ok bool) {
	*r = indexRecord{}
	start := buf
	r.LastKey = key

	var s int
	r.Offset, s = getVarInt(buf)
	if s <= 0 {
		return
	}
	buf = buf[s:]
	return len(start) - len(buf), true
}

func (r *indexRecord) encode(buf []byte, hashSize int) (n int, ok bool) {
	start := buf

	s, ok := putVarInt(buf, uint64(r.Offset))
	if !ok {
		return
	}
	buf = buf[s:]
	return len(start) - len(buf), true
}

var fmtError = errors.New("reftable: format error")

func decodeRestartKey(buf []byte, off uint32) (key string, err error) {
	err = fmtError
	if len(buf) <= int(off) {
		return
	}

	if buf[off] != 0 {
		return
	}
	buf = buf[off+1:]

	l, s := getVarInt(buf)
	if s <= 0 {
		return
	}
	buf = buf[s:]
	l >>= 3
	if uint64(len(buf)) < l {
		return
	}

	return string(buf[:l]), nil
}

func decodeKey(buf []byte, prevKey string) (n int, key string, value uint8, ok bool) {
	start := buf
	prefixLen, s := getVarInt(buf)
	if s <= 0 {
		return
	}
	buf = buf[s:]

	suffixLen, s := getVarInt(buf)
	if s <= 0 {
		return
	}
	buf = buf[s:]
	value = uint8(suffixLen & 0x7)
	suffixLen = suffixLen >> 3

	if int(suffixLen) > len(buf) {
		return
	}

	if int(prefixLen) > len(prevKey) {
		return
	}

	name := make([]byte, suffixLen+prefixLen)
	copy(name, prevKey[:prefixLen])
	copy(name[prefixLen:], buf[:suffixLen])
	buf = buf[suffixLen:]
	return len(start) - len(buf), string(name), value, true
}

func revInt64(t uint64) uint64 {
	return math.MaxUint64 - t
}

func encodeString(buf []byte, val string) (n int, ok bool) {
	start := buf
	if s, ok := putVarInt(buf, uint64(len(val))); !ok {
		return 0, false
	} else {
		buf = buf[s:]
	}
	if len(buf) < len(val) {
		return
	}
	buf = buf[copy(buf, val):]

	return len(start) - len(buf), true
}

func (l *LogRecord) typ() byte {
	return blockTypeLog
}

func (l *LogRecord) key() string {
	var suffix [9]byte
	binary.BigEndian.PutUint64(suffix[1:], revInt64(l.UpdateIndex))
	return l.RefName + string(suffix[:])
}

func (r *LogRecord) copyFrom(in record) {
	*r = *in.(*LogRecord)
}

func (l *LogRecord) String() string {
	return fmt.Sprintf("log(%s, %d)", l.RefName, l.UpdateIndex)
}

func (l *LogRecord) valType() uint8 {
	return 0x1
}

func (l *LogRecord) encode(buf []byte, hashSize int) (n int, fits bool) {
	if l.Old == nil {
		l.Old = make([]byte, hashSize)
	}
	if l.New == nil {
		l.New = make([]byte, hashSize)
	}

	if len(l.Old) != hashSize || len(l.New) != hashSize {
		panic("invalid log entry")
	}

	start := buf

	if len(buf) < len(l.Old) {
		return
	}
	buf = buf[copy(buf, l.Old):]

	if len(buf) < len(l.New) {
		return
	}
	buf = buf[copy(buf, l.New):]

	if s, ok := encodeString(buf, l.Name); !ok {
		return
	} else {
		buf = buf[s:]
	}
	if s, ok := encodeString(buf, l.Email); !ok {
		return
	} else {
		buf = buf[s:]
	}

	if s, ok := putVarInt(buf, l.Time); !ok {
		return
	} else {
		buf = buf[s:]
	}
	if len(buf) < 2 {
		return
	}
	binary.BigEndian.PutUint16(buf, uint16(l.TZOffset))
	buf = buf[2:]

	if s, ok := encodeString(buf, l.Message); !ok {
		return
	} else {
		buf = buf[s:]
	}
	return len(start) - len(buf), true
}

func decodeString(buf []byte) (n int, val string, ok bool) {
	start := buf
	nameLen, s := getVarInt(buf)
	if s <= 0 {
		return
	}
	buf = buf[s:]
	if len(buf) < int(nameLen) {
		return
	}
	val = string(buf[:nameLen])
	buf = buf[nameLen:]
	return len(start) - len(buf), val, true
}

func (l *LogRecord) decodeKey(key string) bool {
	if len(key) < 10 {
		return false
	}
	l.RefName = key[:len(key)-9]
	last := []byte(key[len(key)-9:])
	if last[0] != 0 {
		return false
	}
	l.UpdateIndex = revInt64(binary.BigEndian.Uint64(last[1:]))
	return true
}

func (l *LogRecord) decode(buf []byte, key string, valType uint8, hashSize int) (n int, ok bool) {
	*l = LogRecord{}
	start := buf

	if !l.decodeKey(key) {
		return
	}

	if valType == 0 {
		return 0, true
	}
	buf = buf[n:]

	if len(buf) < 2*hashSize {
		return
	}

	l.Old = make([]byte, hashSize)
	l.New = make([]byte, hashSize)
	copy(l.Old, buf[:hashSize])
	buf = buf[hashSize:]
	copy(l.New, buf[:hashSize])
	buf = buf[hashSize:]

	n, l.Name, ok = decodeString(buf)
	if !ok {
		return
	}
	buf = buf[n:]

	n, l.Email, ok = decodeString(buf)
	if !ok {
		return
	}
	buf = buf[n:]

	l.Time, n = getVarInt(buf)
	if n <= 0 {
		return
	}
	buf = buf[n:]

	if len(buf) < 2 {
		return
	}
	tz := binary.BigEndian.Uint16(buf)
	buf = buf[2:]
	l.TZOffset = int16(tz)

	n, l.Message, ok = decodeString(buf)
	if !ok {
		return
	}
	buf = buf[n:]
	return len(start) - len(buf), true
}
