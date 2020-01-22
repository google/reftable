/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

package reftable

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
)

func testHash(j int) []byte {
	h := bytes.Repeat([]byte("~~~~"), 5)
	binary.BigEndian.PutUint64(h, uint64(j))
	return h
}

func TestRecordRoundTripRefRecord(t *testing.T) {
	inputs := []record{&RefRecord{
		RefName:     "prefix/master",
		UpdateIndex: 32,
	}, &RefRecord{
		RefName:     "prefix/next",
		UpdateIndex: 33,
		Value:       testHash(1),
	}, &RefRecord{
		RefName:     "pre/release",
		UpdateIndex: 33,
		Value:       testHash(1),
		TargetValue: testHash(2),
	}, &RefRecord{
		RefName:     "HEAD",
		UpdateIndex: 34,
		Target:      "prefix/master",
	}}

	testRecordRoundTrip(t, inputs)
}

func testRecordRoundTrip(t *testing.T, inputs []record) {
	typ := inputs[0].typ()
	buf := make([]byte, 1024)
	out := buf

	hashSize := sha1.Size
	lastKey := ""
	for i, in := range inputs {
		n, _, ok := encodeKey(out, lastKey, in.key(), in.valType())
		if !ok {
			t.Fatalf("key encode")
		}
		out = out[n:]
		n, ok = in.encode(out, hashSize)
		if !ok {
			t.Fatalf("encode %d failed", i)
		}
		out = out[n:]
		lastKey = in.key()
	}

	buf = buf[:len(buf)-len(out)]

	lastKey = ""
	var results []record
	for len(buf) > 0 {
		rec := newRecord(typ, "")

		// Check that the value is overwritten
		recVal, ok := quick.Value(reflect.TypeOf(rec), rand.New(rand.NewSource(0)))
		if !ok {
			t.Fatalf("quick.Value failed")
		}
		rec = recVal.Interface().(record)

		n, key, valType, ok := decodeKey(buf, lastKey)
		if !ok {
			t.Fatalf("key decode failed on %v (%q), last %q", buf, buf, lastKey)
		}
		buf = buf[n:]

		n, ok = rec.decode(buf, key, valType, hashSize)
		if !ok {
			t.Fatalf("decode %d failed", len(results))
		}
		buf = buf[n:]
		results = append(results, rec)
		lastKey = key
	}
	if len(results) != len(inputs) {
		t.Fatalf("length: got %d want %d", len(results), len(inputs))
	}
	for i, in := range inputs {
		if !reflect.DeepEqual(in, results[i]) {
			t.Fatalf("result %d: got %#v, want %#v", i, results[i], in)
		}
	}
}

func TestCommonPrefix(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want int
	}{
		{"abc", "ab", 2},
		{"", "abc", 0},
		{"abc", "abd", 2},
		{"abc", "pqr", 0},
	} {
		got := commonPrefixSize(c.a, c.b)
		if got != c.want {
			t.Fatalf("commonPrefixSize(%q,%q): %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestRecordRoundTripLogRecord(t *testing.T) {
	inputs := []record{&LogRecord{
		RefName:     "prefix/master",
		UpdateIndex: 552,
		New:         testHash(2),
		Old:         testHash(1),
		Name:        "C. Omitter",
		Email:       "committer@host.invalid",
		Time:        42,
		TZOffset:    330,
		Message:     "message",
	}, &LogRecord{
		RefName:     "prefix/next",
		UpdateIndex: 551,
		New:         testHash(2),
		Old:         testHash(1),
		Name:        "C. Omitter",
		Email:       "committer@host.invalid",
		Time:        43,
		TZOffset:    330,
		Message:     "message",
	}}

	testRecordRoundTrip(t, inputs)
}

func TestRecordRoundTripObj(t *testing.T) {
	inputs := []record{&objRecord{
		HashPrefix: []byte("prefix/master"),
		Offsets:    []uint64{1, 25, 239},
	}, &objRecord{
		HashPrefix: []byte("prefix/next"),
		Offsets:    []uint64{1, 25, 239, 4932, 5000, 6000, 7000, 8000},
	}, &objRecord{
		HashPrefix: []byte("prefix/next"),
	}}

	testRecordRoundTrip(t, inputs)
}

func TestVarIntRoundtrip(t *testing.T) {
	for _, v := range []uint64{0, 1, 27, 127, 128, 257, 4096, (1 << 64) - 1} {
		var d [10]byte
		n, ok := putVarInt(d[:], v)
		if !ok {
			t.Fatalf("putVarInt(%v): !ok", v)
		}
		w, _ := getVarInt(d[:n])
		if !ok {
			t.Fatalf("getVarInt(%v): !ok", v)
		}

		if v != w {
			t.Errorf("roundtrip: got %v, want %v", w, v)
		}
	}
}
