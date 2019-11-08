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
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestTableObjectIDLen(t *testing.T) {
	h1 := bytes.Repeat([]byte{'~'}, 20)
	h2 := bytes.Repeat([]byte{'~'}, 20)

	h1[4] = 11

	suffix := strings.Repeat("x", 450)
	_, reader := constructTestTable(t, []RefRecord{{
		RefName: "a" + suffix,
		Value:   h1,
	}, {
		RefName: "b" + suffix,
		Value:   h2,
	}}, nil, Options{
		MinUpdateIndex: 0,
		MaxUpdateIndex: 1,
		BlockSize:      512,
	})

	if g := reader.ObjectIDLen; g != 5 {
		t.Errorf("Got %d, want 5", g)
	}

	iter, err := reader.Seek(&objRecord{})
	if err != nil {
		t.Fatalf("start(o): %v", err)
	}

	objResults, err := readIter(BlockTypeObj, iter)
	if err != nil {
		t.Fatalf("readIter(o): %v", err)
	}
	objs := []Record{
		&objRecord{
			HashPrefix: h1[:5],
			Offsets:    []uint64{0},
		},
		&objRecord{
			HashPrefix: h2[:5],
			Offsets:    []uint64{512},
		}}
	if !reflect.DeepEqual(objs, objResults) {
		t.Fatalf("got %v, want %v", objResults, objs)
	}
}

func constructTestTable(t *testing.T, refs []RefRecord, logs []LogRecord, opts Options) (*Writer, *Reader) {
	buf := &bytes.Buffer{}
	var min, max uint64
	min = 0xfffffffff
	for _, r := range refs {
		if r.UpdateIndex < min {
			min = r.UpdateIndex
		}
		if r.UpdateIndex > max {
			max = r.UpdateIndex
		}
	}

	opts.MinUpdateIndex = min
	opts.MaxUpdateIndex = max

	w, err := NewWriter(buf, &opts)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

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
	r, err := NewReader(src)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	if !reflect.DeepEqual(w.Stats.Footer, r.Footer) {
		t.Fatalf("got roundtrip footer %#v, want %#v", w.Stats.Footer, r.Footer)
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

	_, reader := constructTestTable(t, refs, nil, Options{
		MinUpdateIndex: 1,
		MaxUpdateIndex: 1,
		BlockSize:      512,
	})

	_, err := reader.Seek(&RefRecord{})
	if err != nil {
		t.Fatalf("start(r): %v", err)
	}
}

func TestTableRoundTrip(t *testing.T) {
	refs := []RefRecord{{
		RefName:     "HEAD",
		Target:      "refs/heads/master",
		UpdateIndex: 1,
	}, {
		RefName:     "refs/heads/master",
		UpdateIndex: 1,
		Value:       testHash(1),
	}, {
		RefName:     "refs/heads/next",
		UpdateIndex: 1,
		Value:       testHash(2),
	}, {
		RefName:     "refs/tags/release",
		UpdateIndex: 1,
		Value:       testHash(1),
		TargetValue: testHash(2),
	}}
	logs := []LogRecord{{
		RefName: "refs/heads/master",
		TS:      2,
		Old:     testHash(1),
		New:     testHash(2),
		Message: "m2",
	}, {
		RefName: "refs/heads/master",
		TS:      1,
		Old:     testHash(2),
		New:     testHash(1),
		Message: "m1",
	}, {
		RefName: "refs/heads/next",
		TS:      2,
		Old:     testHash(1),
		New:     testHash(2),
		Message: "n2",
	}}

	_, reader := constructTestTable(t, refs, logs, Options{
		MinUpdateIndex: 1,
		MaxUpdateIndex: 1,
		BlockSize:      512,
	})

	iter, err := reader.Seek(&RefRecord{})
	if err != nil {
		t.Fatalf("start(r): %v", err)
	}
	refResults, err := readIter(BlockTypeRef, iter)
	if err != nil {
		t.Fatalf("readIter(r): %v, %v", err, refResults)
	}
	if len(refResults) != len(refs) {
		t.Fatalf("refs size mismatch got %d want %d", len(refResults), len(refs))
	}
	for i, r := range refs {
		var rec Record
		rec = &r
		if !reflect.DeepEqual(refResults[i], rec) {
			t.Fatalf("got %#v, want %#v", refResults[i], rec)
		}
	}

	iter, err = reader.Seek(&LogRecord{})
	if err != nil {
		t.Fatalf("start(g): %v", err)
	}

	logResults, err := readIter(BlockTypeLog, iter)
	if err != nil {
		t.Fatalf("readIter(g): %v", err)
	}
	if len(logResults) != len(logs) {
		t.Fatalf("refs size mismatch got %d want %d", len(logResults), len(logs))
	}
	for i, r := range logs {
		var rec Record
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
		Options{
			MinUpdateIndex:   1,
			SkipIndexObjects: true,
			MaxUpdateIndex:   1,
			BlockSize:        10240,
		})

	if reader.size >= 100 {
		t.Fatalf("got size %d, want < 100", reader.size)
	}
}

func TestTableFirstBlock(t *testing.T) {
	_, reader := constructTestTable(t, []RefRecord{{
		RefName: "hello",
		Value:   testHash(1),
	}}, []LogRecord{
		{
			RefName: "hello",
			New:     testHash(1),
			Old:     testHash(2),
		},
	},
		Options{
			MinUpdateIndex: 1,
			MaxUpdateIndex: 1,
			BlockSize:      256,
		})
	if got := reader.offsets[BlockTypeObj].Offset; got != 256 {
		t.Fatalf("got %d, want %d", got, 256)
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
		case BlockTypeRef:
			rec := RefRecord{
				RefName: name,
				Value:   testHash(i),
			}
			refs = append(refs, rec)
			names = append(names, rec.Key())
		case BlockTypeLog:
			rec := LogRecord{
				RefName: name,
			}
			logs = append(logs, rec)
			names = append(names, rec.Key())
		}
	}

	writer, reader := constructTestTable(t, refs, logs, Options{
		MinUpdateIndex: 1,
		MaxUpdateIndex: 1,
		BlockSize:      blockSize,
	})
	if got := writer.Stats.BlockStats[typ].MaxIndexLevel; got != maxLevel {
		t.Fatalf("got index level %d, want %d", got, maxLevel)
	}

	if sequential {
		iter, err := reader.Seek(newRecord(typ, ""))
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
			if names[i] != r.Key() {
				t.Errorf("record %d: got %q want %q", i, r.Key(), names[i])
			}
		}

		return
	}

	for i := 1; i < len(names); i *= 3 {
		nm := names[i]
		rec := newRecord(typ, names[i])
		it, err := reader.Seek(rec)
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
	testTableSeek(t, BlockTypeRef, 4, 50, 256, 0, false)
}

func TestTableSeekRefLevel1(t *testing.T) {
	// 30 * 50 / 256 = 6, this will force a 1-level index
	testTableSeek(t, BlockTypeRef, 30, 50, 256, 1, false)
}

func TestTableSeekRefLevel2(t *testing.T) {
	// 150 * 50 / 256 ~= 30, this should have 2-level index
	testTableSeek(t, BlockTypeRef, 120, 50, 256, 2, false)
}

func TestTableSeekLogLevel0(t *testing.T) {
	// 8 * 50 / 256 ~= 2, this should not have an index
	testTableSeek(t, BlockTypeLog, 4, 50, 256, 0, false)
}

func TestTableIterLogLevel0(t *testing.T) {
	// 4 * 50 / 256 ~= 2, this should not have an index
	testTableSeek(t, BlockTypeLog, 4, 50, 256, 0, true)
}

func TestTableIterRefLevel0(t *testing.T) {
	// 4 * 50 / 256 ~= 2, this should not have an index
	testTableSeek(t, BlockTypeRef, 4, 50, 256, 0, true)
}

func TestTableSeekLogLevel1(t *testing.T) {
	// 25 * (50b + 60b) -> 13 blocks
	// 13 blocks -> 3 index blocks; not enough for another index level
	testTableSeek(t, BlockTypeLog, 25, 50, 256, 1, false)
}

func TestTableLogBlocksUnpadded(t *testing.T) {
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
		Options{
			BlockSize: 4096,
		})
	if got := writer.Stats.BlockStats[BlockTypeLog].Blocks; got != N {
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
		Options{
			BlockSize:        256,
			SkipIndexObjects: !indexed,
		})

	t1 := testHash(4)

	var want []Record
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

	got, err := readIter(BlockTypeRef, it)
	if err != nil {
		t.Fatalf("ReadIter: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got value refs %v want %v", got, want)
	}
}

func TestTableMinUpdate(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := NewWriter(buf, &Options{
		MinUpdateIndex: 2,
		MaxUpdateIndex: 4,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

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
