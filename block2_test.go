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
	"fmt"
	"reflect"
	"testing"
)

func TestBlock2ReadWrite(t *testing.T) {
	var refs []RefRecord
	for i := 0; i < 100; i++ {
		rec := RefRecord{
			RefName: fmt.Sprintf("ref/branch%03d", i),
		}
		switch i % 3 {
		case 0:
			rec.Target = fmt.Sprintf("ref/branch%03d", i%6)
		case 1:
			rec.Value = testHash(1)
		}

		refs = append(refs, rec)
	}

	block := make([]byte, 10240)
	bw := newBlockWriter2(block)

	for _, r := range refs {
		if !bw.add(&r) {
			t.Fatalf("add %v failed", r)
		}
	}

	finished := bw.finish()

	br := newBlockReader2(finished)
	it := br.start()
	i := 0
	for {
		var ref RefRecord
		ok, err := it.Next(&ref)
		if err != nil {
			t.Fatalf("next %d: %v", i, err)
		}

		if !ok {
			break
		}

		if !reflect.DeepEqual(ref, refs[i]) {
			t.Errorf("%d: got %v want %v", i, ref, refs[i])
		}
		i++
	}

	for _, r := range refs {
		it, err := br.Seek(&r)
		if err != nil {
			t.Fatal(err)
		}
		var got RefRecord

		ok, err := it.Next(&got)
		if err != nil {
			t.Fatal(err)
		}

		if !ok {
			t.Errorf("next failed")
		}

		if !reflect.DeepEqual(got, r) {
			t.Errorf("seek: got %v, want %v", got, r)
		}
	}
}

func TestBlock2ReadWriteOne(t *testing.T) {
	block := make([]byte, 10240)
	bw := newBlockWriter2(block)
	rec := RefRecord{
		RefName: "branch",
		Target:  "target",
	}
	if !bw.add(&rec) {
		t.Fatalf("add %v failed", rec)
	}

	finished := bw.finish()
	br := newBlockReader2(finished)
	it := br.start()

	var got RefRecord
	ok, err := it.Next(&got)
	if err != nil {
		t.Fatalf("next %v", err)
	}

	if !ok {
		t.Fatalf("!ok")
	}

	if !reflect.DeepEqual(got, rec) {
		t.Errorf("got %v want %v", got, rec)
	}
}
