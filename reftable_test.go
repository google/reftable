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
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestTableObjectIDLen(t *testing.T) {
	suffix := strings.Repeat("x", 450)

	var refs []RefRecord

	obj2ref := map[string]string{}
	for i := 0; i < 8; i++ {
		h := bytes.Repeat([]byte{'~'}, sha1.Size)
		h[4] = byte(i)

		refName := string('a'+i) + suffix
		refs = append(refs, RefRecord{
			RefName: refName,
			Value:   h,
		})

		obj2ref[string(h)] = refName
	}

	_, reader := constructTestTable(t, refs, nil, Config{
		BlockSize: 512,
	})
	if g := reader.objectIDLen; g != 5 {
		t.Errorf("Got %d, want 5", g)
	}

	iter, err := reader.seek(&objRecord{})
	if err != nil {
		t.Fatalf("start(o): %v", err)
	}

	obj := objRecord{}
	ok, err := iter.Next(&obj)
	// Check that the index is there.
	if !ok || err != nil {
		t.Fatalf("objRecord.Next: %v %v", ok, err)
	}

	for h, r := range obj2ref {
		iter, err := reader.RefsFor([]byte(h))
		if err != nil {
			t.Fatalf("RefsFor %q: %v", h, err)
		}
		var ref RefRecord
		ok, err := iter.NextRef(&ref)
		if !ok || err != nil {
			t.Fatalf("Next: %v %v", ok, err)
		}

		if ref.RefName != r {
			t.Fatalf("got ref %q, want %q", ref.RefName, r)
		}
	}
}

func constructTestTable(t *testing.T, refs []RefRecord, logs []LogRecord, cfg Config) (*Writer, *Reader) {
	buf := &bytes.Buffer{}
	var min, max uint64
	min = math.MaxUint64
	for _, r := range refs {
		if r.UpdateIndex < min {
			min = r.UpdateIndex
		}
		if r.UpdateIndex > max {
			max = r.UpdateIndex
		}
	}

	w, err := NewWriter(buf, &cfg)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	w.SetLimits(min, max)
	for _, r := range refs {
		if err := w.AddRef(&r); err != nil {
			t.Fatalf("AddRef: %v", err)
		}
	}
	for _, l := range logs {
		if err := w.AddLog(&l); err != nil {
			t.Fatalf("AddLog: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	src := &ByteBlockSource{buf.Bytes()}
	r, err := NewReader(src, "buffer")
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	return w, r
}

func TestTableSeekEmpty(t *testing.T) {
	refs := []RefRecord{{
		RefName:     "HEAD",
		Target:      "refs/heads/master",
		UpdateIndex: 1,
	}, {
		RefName:     "refs/heads/master",
		UpdateIndex: 1,
		Value:       testHash(1),
	}}

	_, reader := constructTestTable(t, refs, nil, Config{
		BlockSize: 512,
	})

	_, err := reader.SeekRef("")
	if err != nil {
		t.Fatalf("start(r): %v", err)
	}
}

func TestTableRoundTripSHA256(t *testing.T) {
	testTableRoundTrip(t, SHA256ID)
}

func TestTableRoundTripSHA1(t *testing.T) {
	testTableRoundTrip(t, SHA1ID)
}

func testTableRoundTrip(t *testing.T, hashID HashID) {
	genHash := testHash
	if hashID == SHA256ID {
		genHash = testHash256
	}
	refs := []RefRecord{{
		RefName:     "HEAD",
		Target:      "refs/heads/master",
		UpdateIndex: 1,
	}, {
		RefName:     "refs/heads/master",
		UpdateIndex: 1,
		Value:       genHash(1),
	}, {
		RefName:     "refs/heads/next",
		UpdateIndex: 1,
		Value:       genHash(2),
	}, {
		RefName:     "refs/tags/release",
		UpdateIndex: 1,
		Value:       genHash(1),
		TargetValue: genHash(2),
	}}
	logs := []LogRecord{{
		RefName:     "refs/heads/master",
		UpdateIndex: 2,
		Old:         genHash(1),
		New:         genHash(2),
		Message:     "m2",
	}, {
		RefName:     "refs/heads/master",
		UpdateIndex: 1,
		Old:         genHash(2),
		New:         genHash(1),
		Message:     "m1",
	}, {
		RefName:     "refs/heads/next",
		UpdateIndex: 2,
		Old:         genHash(1),
		New:         genHash(2),
		Message:     "n2",
	}}

	_, reader := constructTestTable(t, refs, logs, Config{
		BlockSize: 512,
		HashID:    hashID,
	})

	iter, err := reader.SeekRef("")
	if err != nil {
		t.Fatalf("start(r): %v", err)
	}
	refResults, err := readIter(blockTypeRef, iter.impl)
	if err != nil {
		t.Fatalf("readIter(r): %v, %v", err, refResults)
	}
	if len(refResults) != len(refs) {
		t.Fatalf("refs size mismatch got %d want %d", len(refResults), len(refs))
	}
	for i, r := range refs {
		var rec record
		rec = &r
		if !reflect.DeepEqual(refResults[i], rec) {
			t.Fatalf("got %#v, want %#v", refResults[i], rec)
		}
	}

	iter, err = reader.SeekLog("", math.MaxUint64)
	if err != nil {
		t.Fatalf("start(g): %v", err)
	}

	logResults, err := readIter(blockTypeLog, iter.impl)
	if err != nil {
		t.Fatalf("readIter(g): %v", err)
	}
	if len(logResults) != len(logs) {
		t.Fatalf("refs size mismatch got %d want %d", len(logResults), len(logs))
	}
	for i, r := range logs {
		var rec record
		rec = &r
		if !reflect.DeepEqual(logResults[i], rec) {
			t.Fatalf("got %#v, want %#v", logResults[i], rec)
		}
	}
}

func TestTableLastBlockLacksPadding(t *testing.T) {
	_, reader := constructTestTable(t, []RefRecord{{
		RefName: "hello",
		Value:   testHash(1),
	}}, nil,
		Config{
			SkipIndexObjects: true,
			BlockSize:        10240,
		})

	if reader.size >= 100 {
		t.Fatalf("got size %d, want < 100", reader.size)
	}
}

func TestSmallTableLacksPadding(t *testing.T) {
	// A small table doesn't have obj-index, so it doesnt have any padding.
	_, reader := constructTestTable(t, []RefRecord{{
		RefName: "hello",
		Value:   testHash(1),
	}}, []LogRecord{{
		RefName: "hello",
		Old:     testHash(0),
		New:     testHash(1),
		Name:    "John Doe",
		Email:   "j.doe@aol.com",
		Time:    uint64(time.Date(2018, 10, 10, 14, 9, 53, 0, time.UTC).Unix()),
	}},
		Config{
			BlockSize: 1024,
		})

	if reader.size >= 500 {
		t.Fatalf("got size %d, want < 500", reader.size)
	}
}

func testTableSeek(t *testing.T, typ byte, recCount, recSize int, blockSize uint32, maxLevel int, sequential bool) {
	var refs []RefRecord
	var logs []LogRecord
	var names []string

	suffix := strings.Repeat("x", recSize)
	for i := 0; i < recCount; i++ {
		// Put the variable bit in front to kill prefix compression
		name := fmt.Sprintf("%04d/%s", i, suffix)[:recSize]
		switch typ {
		case blockTypeRef:
			rec := RefRecord{
				RefName: name,
				Value:   testHash(i),
			}
			refs = append(refs, rec)
			names = append(names, rec.key())
		case blockTypeLog:
			rec := LogRecord{
				RefName: name,
			}
			logs = append(logs, rec)
			names = append(names, rec.key())
		}
	}

	writer, reader := constructTestTable(t, refs, logs, Config{
		BlockSize: blockSize,
	})
	if got := writer.getBlockStats(typ).MaxIndexLevel; got != maxLevel {
		t.Fatalf("got index level %d, want %d", got, maxLevel)
	}

	if sequential {
		iter, err := reader.seek(newRecord(typ, ""))
		if err != nil {
			t.Fatalf("Start %v", err)
		}
		recs, err := readIter(typ, iter)
		if err != nil {
			t.Fatalf("readIter %v", err)
		}

		if len(recs) != recCount {
			t.Fatalf("gotr %d, want %d records: %v", len(recs), recCount, recs)
		}

		for i, r := range recs {
			if names[i] != r.key() {
				t.Errorf("record %d: got %q want %q", i, r.key(), names[i])
			}
		}

		return
	}

	for i := 1; i < len(names); i *= 3 {
		nm := names[i]
		rec := newRecord(typ, names[i])
		it, err := reader.seek(rec)
		if err != nil {
			t.Errorf("Seek %q: %v", nm, err)
			continue
		}
		var ref RefRecord
		ok, err := it.Next(&ref)
		if err != nil {
			t.Errorf("Next %q: %v", nm, err)
			continue
		}
		if !ok {
			t.Errorf("Next %q: want ok", nm)
			continue
		}

		if ref.RefName != nm {
			t.Errorf("got %q want %q", ref.RefName, nm)
		}
	}
}

func TestTableSeekRefLevel0(t *testing.T) {
	// 8 * 50 / 256 ~= 2, this should not have an index
	testTableSeek(t, blockTypeRef, 4, 50, 256, 0, false)
}

func TestTableSeekRefLevel1(t *testing.T) {
	// 30 * 50 / 256 = 6, this will force a 1-level index
	testTableSeek(t, blockTypeRef, 30, 50, 256, 1, false)
}

func TestTableSeekRefLevel2(t *testing.T) {
	// 150 * 50 / 256 ~= 30, this should have 2-level index
	testTableSeek(t, blockTypeRef, 120, 50, 256, 2, false)
}

func TestTableSeekLogLevel0(t *testing.T) {
	// 8 * 50 / 256 ~= 2, this should not have an index
	testTableSeek(t, blockTypeLog, 4, 50, 256, 0, false)
}

func TestTableIterLogLevel0(t *testing.T) {
	// 4 * 50 / 256 ~= 2, this should not have an index
	testTableSeek(t, blockTypeLog, 4, 50, 256, 0, true)
}

func TestTableIterRefLevel0(t *testing.T) {
	// 4 * 50 / 256 ~= 2, this should not have an index
	testTableSeek(t, blockTypeRef, 4, 50, 256, 0, true)
}

func TestTableSeekLogLevel1(t *testing.T) {
	// 25 * (50b + 60b) -> 13 blocks
	// 13 blocks -> 3 index blocks; not enough for another index level
	testTableSeek(t, blockTypeLog, 25, 50, 256, 1, false)
}

func TestTableLogBlocksUnaligned(t *testing.T) {
	var ls []LogRecord

	N := 50
	for i := 0; i < N; i++ {
		ls = append(ls,
			LogRecord{
				RefName: fmt.Sprintf("%04d", i),
				Message: strings.Repeat("x", 4000),
			})
	}

	writer, reader := constructTestTable(t, nil, ls,
		Config{
			BlockSize: 4096,
		})
	if got := writer.Stats.LogStats.Blocks; got != N {
		t.Errorf("got log block count %d, want %d", got, N)
	}
	if reader.size > 4000 {
		t.Fatalf("got size %d, want < 4000", reader.size)
	}
}

func TestTableRefsForIndexed(t *testing.T) {
	testTableRefsFor(t, true)
}

func TestTableRefsForLinear(t *testing.T) {
	testTableRefsFor(t, false)
}

func testTableRefsFor(t *testing.T, indexed bool) {
	var refs []RefRecord

	for i := 0; i < 50; i++ {
		refs = append(refs, RefRecord{
			RefName:     fmt.Sprintf("%04d/%s", i, strings.Repeat("x", 50))[:40],
			Value:       testHash(i / 4),
			TargetValue: testHash(3 + i/4),
		})
	}

	_, reader := constructTestTable(t, refs, nil,
		Config{
			BlockSize:        256,
			SkipIndexObjects: !indexed,
		})

	t1 := testHash(4)

	var want []record
	for _, r := range refs {
		if bytes.Compare(r.Value, t1) == 0 || bytes.Compare(r.TargetValue, t1) == 0 {
			copy := r
			want = append(want, &copy)
		}
	}

	it, err := reader.RefsFor(t1)
	if err != nil {
		t.Fatalf("RefsFor: %v", err)
	}

	got, err := readIter(blockTypeRef, it.impl)
	if err != nil {
		t.Fatalf("ReadIter: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got value refs %v want %v", got, want)
	}
}

func TestTableMinUpdate(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := NewWriter(buf, &Config{})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	w.SetLimits(2, 4)
	for _, ref := range []RefRecord{{
		RefName:     "ref",
		UpdateIndex: 1,
	}, {
		RefName:     "ref",
		UpdateIndex: 5,
	}} {
		if err := w.AddRef(&ref); err == nil {
			t.Fatalf("succeeded adding ref @ %d outside bounds.", ref.UpdateIndex)
		}
	}
}

func TestTableUpdateIndexAcrossBlockBoundary(t *testing.T) {
	records := []RefRecord{{
		RefName:     fmt.Sprintf("A%0*d", 200, 0),
		UpdateIndex: 2,
	}, {
		RefName:     fmt.Sprintf("B%0*d", 200, 0),
		UpdateIndex: 2,
	}}
	w, r := constructTestTable(t, records, nil, Config{
		BlockSize: 256,
	})
	if got, want := w.Stats.RefStats.Blocks, 2; got != want {
		t.Fatalf("got %d blocks want %d", got, want)
	}

	if got, want := w.Stats.RefStats.Entries, 2; got != want {
		t.Fatalf("got %d entries want %d", got, want)
	}

	it, err := r.SeekRef("B")
	if err != nil {
		t.Fatalf("SeekRef: %v", err)
	}

	var ref RefRecord
	ok, err := it.NextRef(&ref)
	if err != nil || !ok {
		t.Fatalf("Next: %v, %v", ok, err)
	}

	if got, want := ref.UpdateIndex, records[1].UpdateIndex; got != want {
		t.Fatalf("got UpdateIndex %d, want %d", got, want)
	}
}

func TestUnalignedBlock(t *testing.T) {
	records := []RefRecord{{
		RefName:     fmt.Sprintf("A%0*d", 200, 0),
		UpdateIndex: 2,
	}, {
		RefName:     fmt.Sprintf("B%0*d", 200, 0),
		UpdateIndex: 2,
	}}

	var logs []LogRecord
	for i := 0; i < 10; i++ {
		logs = append(logs, LogRecord{
			RefName: fmt.Sprintf("branch%02d", i),
			Message: strings.Repeat("x", 160),
		})
	}
	w, r := constructTestTable(t, records, logs, Config{
		BlockSize: 256,
	})

	if got, want := w.Stats.RefStats.Blocks, 2; got != want {
		t.Fatalf("got %d blocks want %d", got, want)
	}

	it, err := r.SeekRef("B")
	if err != nil {
		t.Fatalf("SeekRef: %v", err)
	}

	var ref RefRecord
	ok, err := it.NextRef(&ref)
	if err != nil || !ok {
		t.Fatalf("Next: %v, %v", ok, err)
	}

	if _, err := it.NextRef(&ref); err != nil {
		t.Fatalf("Next: %v", err)
	}
}
