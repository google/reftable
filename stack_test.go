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
	"io/ioutil"
	"log"
	"os"
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
	log.Println("dir", dir)
	if err := os.Mkdir(dir+"/reftable", 0755); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		Unaligned: true,
	}

	st, err := NewStack(dir+"/reftable", dir+"/refs", cfg)
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
		it, err := m.SeekRef(&RefRecord{RefName: k})
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
