/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

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
		res = append(res, r.rec.key())
	}

	if !reflect.DeepEqual(names, res) {
		t.Errorf("got \n%v want \n%v", res, names)
	}
}

func constructMergedRefTestTable(t *testing.T, recs ...[]RefRecord) *Merged {
	var tabs []*Reader
	for _, rec := range recs {
		opts := Config{}

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

	{
		want := []record{
			// the deletion is also produced in the merged
			// iteration, for compaction.
			&r2[0],
			&r1[1],
			&r3[0],
			&r3[1],
		}

		iter, err := merged.SeekRef("a")
		if err != nil {
			t.Fatalf("Seek: %v", err)
		}

		got, err := readIter(blockTypeRef, iter.impl)
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

		got, err := readIter(blockTypeRef, iter.impl)
		if err != nil {
			t.Fatalf("readIter: %v", err)
		}

		want := []record{
			&r1[1],
			&r3[1],
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %#v", got, want)
		}
	}

}
