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
	"math/rand"
	"reflect"
	"testing"
)

func TestPQ(t *testing.T) {
	pq := mergedIterPQueue{}

	rec := func(k string) pqEntry {
		return pqEntry{rec: &RefRecord{
			RefName: k,
		},
			index: 0,
		}
	}

	var names []string
	for i := 0; i < 30; i++ {
		names = append(names, fmt.Sprintf("%02d", i))
	}

	for _, j := range rand.Perm(len(names)) {
		pq.add(rec(names[j]))
		pq.check()
	}

	var res []string
	for !pq.isEmpty() {
		r := pq.remove()
		pq.check()
		res = append(res, r.rec.Key())
	}

	if !reflect.DeepEqual(names, res) {
		t.Errorf("got \n%v want \n%v", res, names)
	}
}

func constructMergedRefTestTable(t *testing.T, recs ...[]RefRecord) *Merged {
	var tabs []*Reader
	for _, rec := range recs {
		opts := Options{}

		_, reader := constructTestTable(t, rec, nil, opts)
		tabs = append(tabs, reader)
	}

	m, err := NewMerged(tabs)
	if err != nil {
		t.Fatalf("NewMerged: %v", err)
	}

	return m
}

func TestMerged(t *testing.T) {
	r1 := []RefRecord{{
		RefName:     "a",
		UpdateIndex: 1,
		Value:       testHash(1),
	}, {
		RefName:     "b",
		UpdateIndex: 1,
		Value:       testHash(1),
	}, {
		RefName:     "c",
		UpdateIndex: 1,
		Value:       testHash(1),
	}}

	r2 := []RefRecord{{
		RefName:     "a",
		UpdateIndex: 2,
	}}

	r3 := []RefRecord{{
		RefName:     "c",
		UpdateIndex: 3,
		Value:       testHash(2),
	}, {
		RefName:     "d",
		UpdateIndex: 3,
		Value:       testHash(1),
	}}

	merged := constructMergedRefTestTable(t, r1, r2, r3)

	if false {
		want := []Record{
			// the deletion is also produced in the merged
			// iteration, for compaction.
			&r2[0],
			&r1[1],
			&r3[0],
			&r3[1],
		}

		iter, err := merged.Seek(&RefRecord{RefName: "a"})
		if err != nil {
			t.Fatalf("Seek: %v", err)
		}

		got, err := readIter(BlockTypeRef, iter)
		if err != nil {
			t.Fatalf("readIter: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v, want %#v", got, want)
		}
	}

	{
		iter, err := merged.RefsFor(testHash(1))
		if err != nil {
			t.Fatalf("Seek: %v", err)
		}

		got, err := readIter(BlockTypeRef, iter)
		if err != nil {
			t.Fatalf("readIter: %v", err)
		}

		want := []Record{
			&r1[1],
			&r3[1],
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %#v", got, want)
		}
	}

}
