/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

package reftable

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestStack(t *testing.T) {
	testStackN(t, 33)
}

func testStackN(t *testing.T, N int) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir+"/reftable", 0755); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		Unaligned: true,
	}

	st, err := NewStack(dir+"/reftable", cfg)
	if err != nil {
		t.Fatal(err)
	}

	refmap := map[string][]byte{}
	for i := 0; i < N; i++ {
		if err := st.Add(func(w *Writer) error {
			r := RefRecord{
				RefName:     fmt.Sprintf("branch%02d", i),
				Value:       testHash(i),
				UpdateIndex: uint64(i) + 1,
			}
			w.SetLimits(uint64(i)+1, uint64(i)+1)
			refmap[r.RefName] = r.Value
			if err := w.AddRef(&r); err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	m := st.Merged()
	for k, v := range refmap {
		it, err := m.SeekRef(k)
		if err != nil {
			t.Fatal(err)
		}

		var r RefRecord
		ok, err := it.NextRef(&r)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("not found: %v", k)
		}

		if bytes.Compare(r.Value, v) != 0 {
			t.Fatalf("value does not match got %q want %q", r.Value, v)
		}
	}

	if st.Stats.Failures != 0 {
		t.Fatalf("got %d failures", st.Stats.Failures)
	}

	if limit := N * log2(uint64(N)); st.Stats.Attempts > limit {
		t.Fatalf("got %d compactions, want max %d", st.Stats.Attempts, limit)
	}
}

func TestAutoCompaction(t *testing.T) {
	const N = 1000
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}

	st, err := NewStack(dir, Config{})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < N; i++ {
		if err := st.Add(func(w *Writer) error {
			r := RefRecord{
				RefName:     fmt.Sprintf("branch%04d", i),
				Target:      "target",
				UpdateIndex: st.NextUpdateIndex(),
			}
			w.SetLimits(r.UpdateIndex, r.UpdateIndex)
			if err := w.AddRef(&r); err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}

		if i < 3 {
			continue
		}
		if got, limit := len(st.stack), 2*log2(uint64(i)); got > limit {
			t.Fatalf("stack is %d long, more than limit %d", got, limit)
		}
	}

	if got, limit := st.Stats.EntriesWritten, uint64(log2(N)*N); got > limit {
		t.Fatalf("wrote %d entries, more than limit %d", got, limit)
	}
}

func TestMixedHashSize(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir+"/reftable", 0755); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		Unaligned: true,
		HashID:    SHA1ID,
	}

	st, err := NewStack(dir+"/reftable", cfg)
	if err != nil {
		t.Fatal(err)
	}

	N := 2
	for i := 0; i < N; i++ {
		if err := st.Add(func(w *Writer) error {
			r := RefRecord{
				RefName:     "branch",
				UpdateIndex: uint64(i) + 1,
				Value:       testHash(i),
			}

			w.SetLimits(uint64(i)+1, uint64(i)+1)
			return w.AddRef(&r)
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	defaultConf := Config{}
	if defaultSt, err := NewStack(dir+"/reftable", defaultConf); err != nil {
		t.Fatalf("NewStack(defaultConf): %v", err)
	} else {
		defaultSt.Close()
	}

	cfg2 := cfg
	cfg2.HashID = SHA256ID
	if _, err := NewStack(dir+"/reftable", cfg2); err == nil {
		t.Fatal("got success; want an error for hash size mismatch")
	}
}

func TestTombstones(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir+"/reftable", 0755); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		Unaligned: true,
	}

	st, err := NewStack(dir+"/reftable", cfg)
	if err != nil {
		t.Fatal(err)
	}

	st.disableAutoCompact = true

	N := 30 // must be even
	refmap := map[string][]byte{}
	for i := 0; i < N; i++ {
		if err := st.Add(func(w *Writer) error {
			r := RefRecord{
				RefName:     "branch",
				UpdateIndex: uint64(i) + 1,
			}
			if i%2 == 0 {
				r.Value = testHash(i)
			}

			w.SetLimits(uint64(i)+1, uint64(i)+1)
			refmap[r.RefName] = r.Value
			if err := w.AddRef(&r); err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	if r, err := ReadRef(st.Merged(), "branch"); err != nil {
		t.Fatalf("ReadRef: %v", err)
	} else if r != nil {
		t.Fatalf("got record %v", r)
	}

	if err := st.CompactAll(nil); err != nil {
		t.Fatal(err)
	}

	if r, err := ReadRef(st.Merged(), "branch"); err != nil {
		t.Fatalf("ReadRef: %v", err)
	} else if r != nil {
		t.Fatalf("got record %v", r)
	}
}

func TestSuggestCompactionSegment(t *testing.T) {
	sizes := []uint64{128, 64, 17, 16, 9, 9, 9, 16, 16}
	min := suggestCompactionSegment(sizes)

	if min.start != 2 || min.end != 7 {
		t.Fatalf("got seg %v, want [2,7)", min)
	}
}

func TestCompactionReflogExpiry(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir+"/reftable", 0755); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		Unaligned: true,
	}

	st, err := NewStack(dir+"/reftable", cfg)
	if err != nil {
		t.Fatal(err)
	}

	N := 20
	logmap := map[int]*LogRecord{}
	for i := 1; i < N; i++ {
		if err := st.Add(func(w *Writer) error {
			log := LogRecord{
				RefName:     fmt.Sprintf("branch%02d", i),
				New:         testHash(i),
				UpdateIndex: uint64(i),
				Time:        uint64(i),
			}
			w.SetLimits(uint64(i), uint64(i))
			logmap[i] = &log
			if err := w.AddLog(&log); err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	logConfig := LogExpirationConfig{
		Time: 10,
	}

	if err := st.CompactAll(&logConfig); err != nil {
		t.Fatalf("CompactAll: %v", err)
	}

	have := func(i int) bool {
		name := logmap[i].RefName

		var max uint64
		max = ^max
		logRec, err := ReadLogAt(st.Merged(), name, max)
		if err != nil {
			t.Fatalf("ReadLogAt %v", err)
		}
		return logRec != nil
	}

	if !have(11) {
		t.Fatalf("misses entry @21")
	}
	if have(9) {
		t.Fatalf("has entry @19")
	}

	logConfig = LogExpirationConfig{MinUpdateIndex: 15}
	if err := st.CompactAll(&logConfig); err != nil {
		t.Fatalf("CompactAll: %v", err)
	}

	it, err := st.Merged().SeekLog("", 0xffffff)
	for {
		var l LogRecord
		ok, err := it.NextLog(&l)
		if !ok || err != nil {
			break
		}
	}

	if have(14) {
		t.Fatalf("has log entry @14")
	}

	if !have(16) {
		t.Fatalf("misses log entry @16")
	}
}

func TestIgnoreEmptyTables(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir+"/reftable", 0755); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		Unaligned: true,
	}

	st, err := NewStack(dir+"/reftable", cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := st.Add(func(w *Writer) error {
		w.SetLimits(1, 1)
		return nil
	}); err != nil {
		t.Fatal("Add", err)
	}

	entries, err := ioutil.ReadDir(dir + "/reftable")
	if err != nil {
		t.Fatal("ReadDir", err)
	} else if len(entries) != 0 {
		var ss []string
		for _, e := range entries {
			ss = append(ss, e.Name())
		}
		t.Fatalf("got: %v", strings.Join(ss, " "))
	}
}

func TestNameCheck(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir+"/reftable", 0755); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		Unaligned: true,
	}

	st, err := NewStack(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := st.Add(func(w *Writer) error {
		next := st.NextUpdateIndex()
		r := RefRecord{
			RefName:     "branch",
			UpdateIndex: next,
			Value:       testHash(1),
		}

		w.SetLimits(next, next)
		if err := w.AddRef(&r); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("write %v", err)
	}

	if err := st.Add(func(w *Writer) error {
		next := st.NextUpdateIndex()
		r := RefRecord{
			RefName:     "branch/dir",
			UpdateIndex: next,
			Value:       testHash(2),
		}

		w.SetLimits(next, next)
		if err := w.AddRef(&r); err != nil {
			return err
		}
		return nil
	}); err == nil {
		t.Fatalf("should have failed to add dir/file conflict")
	}
}

func TestLogLine(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir+"/reftable", 0755); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		ExactLogMessage: false,
	}

	st, err := NewStack(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := st.Add(func(w *Writer) error {
		next := st.NextUpdateIndex()
		l := LogRecord{
			RefName:     "branch",
			UpdateIndex: next,
			New:         testHash(1),
			Old:         testHash(2),
			Message:     "a\nb",
		}

		w.SetLimits(next, next)
		if err := w.AddLog(&l); err != nil {
			return err
		}
		return nil
	}); err == nil {
		t.Fatalf("should have failed adding multiline log message")
	}

	updateIndex := uint64(42)
	if err := st.Add(func(w *Writer) error {
		next := st.NextUpdateIndex()
		l := LogRecord{
			RefName:     "branch",
			UpdateIndex: updateIndex,
			New:         testHash(1),
			Old:         testHash(2),
			Message:     "message",
		}

		w.SetLimits(next, next)
		if err := w.AddLog(&l); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	m := st.Merged()
	it, err := m.SeekLog("branch", updateIndex)
	if err != nil {
		t.Errorf("SeekLog: %v", err)
	}

	var log LogRecord
	ok, err := it.NextLog(&log)
	if !ok || err != nil {
		t.Errorf("SeekLog: %v %v", ok, err)
	}

	if got, want := log.Message, "message\n"; got != want {
		t.Errorf("got %q want %q", got, want)
	}
}
