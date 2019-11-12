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
	"log"
	"sort"
)

// pqEntry is an entry of the priority queue
type pqEntry struct {
	rec   record
	index int
}

func (e pqEntry) String() string {
	return fmt.Sprintf("%s,%d", e.rec.key(), e.index)
}

func pqLess(a, b pqEntry) bool {
	if a.rec.key() == b.rec.key() {
		return a.index > b.index
	}

	return a.rec.key() < b.rec.key()
}

// a min heap
type mergedIterPQueue struct {
	heap []pqEntry
}

func (pq *mergedIterPQueue) isEmpty() bool {
	return len(pq.heap) == 0
}

func (pq *mergedIterPQueue) top() pqEntry {
	return pq.heap[0]
}

func (pq *mergedIterPQueue) check() {
	for i := 1; i < len(pq.heap); i++ {
		par := (i - 1) / 2

		if !pqLess(pq.heap[par], pq.heap[i]) {
			log.Panicf("%v %v %d", pq.heap[par], pq.heap[i], i)
		}
	}
}

func (pq *mergedIterPQueue) remove() pqEntry {
	e := pq.heap[0]

	pq.heap[0] = pq.heap[len(pq.heap)-1]
	pq.heap = pq.heap[:len(pq.heap)-1]

	i := 0
	for i < len(pq.heap) {
		min := i
		j, k := 2*i+1, 2*i+2

		if j < len(pq.heap) && pqLess(pq.heap[j], pq.heap[i]) {
			min = j
		}
		if k < len(pq.heap) && pqLess(pq.heap[k], pq.heap[min]) {
			min = k
		}

		if min == i {
			break
		}

		pq.heap[i], pq.heap[min] = pq.heap[min], pq.heap[i]
		i = min
	}

	return e
}

func (pq *mergedIterPQueue) add(e pqEntry) {
	pq.heap = append(pq.heap, e)
	i := len(pq.heap) - 1
	for i > 0 {
		j := (i - 1) / 2

		if pqLess(pq.heap[j], pq.heap[i]) {

			break
		}

		pq.heap[j], pq.heap[i] = pq.heap[i], pq.heap[j]
		i = j
	}
}

// Merged is a stack of reftables.
type Merged struct {
	stack []*Reader
}

// NewMerged creates a reader for a merged reftable.
func NewMerged(tabs []*Reader) (*Merged, error) {
	var last Table
	for i, t := range tabs {
		if last != nil && last.MaxUpdateIndex() >= t.MinUpdateIndex() {
			return nil, fmt.Errorf("reftable: table %d has min %d, table %d has max %d; indices must be increasing.", i, t.MinUpdateIndex(), i-1, last.MaxUpdateIndex())
		}
		last = t
	}

	return &Merged{tabs}, nil
}

// MaxUpdateIndex implements the Table interface.
func (m *Merged) MaxUpdateIndex() uint64 {
	return m.stack[len(m.stack)].MaxUpdateIndex()
}

// MinUpdateIndex implements the Table interface.
func (m *Merged) MinUpdateIndex() uint64 {
	return m.stack[0].MinUpdateIndex()
}

// RefsFor returns the refs that point to the given oid
func (m *Merged) RefsFor(oid []byte) (*Iterator, error) {
	mit := &mergedIter{
		typ: blockTypeRef,
	}
	for _, t := range m.stack {
		it, err := t.RefsFor(oid)
		if err != nil {
			return nil, err
		}
		mit.stack = append(mit.stack, it.impl)
	}

	if err := mit.init(); err != nil {
		return nil, err
	}
	return &Iterator{&filteringRefIterator{
		tab:         m,
		oid:         oid,
		it:          mit,
		doubleCheck: true,
	}}, nil
}

func uniq(ss []string) []string {
	sort.Strings(ss)
	u := ss[:0]
	last := ""
	for i, s := range ss {
		if i == 0 || last != s {
			u = append(u, s)
		}

		last = s
	}
	return u
}

// Seek returns an iterator positioned before the wanted record.
func (m *Merged) SeekLog(log *LogRecord) (*Iterator, error) {
	impl, err := m.seek(log)
	if err != nil {
		return nil, err
	}
	return &Iterator{impl}, nil
}

func (m *Merged) SeekRef(ref *RefRecord) (*Iterator, error) {
	impl, err := m.seek(ref)
	if err != nil {
		return nil, err
	}
	return &Iterator{impl}, nil
}

func (m *Merged) seek(rec record) (iterator, error) {
	var its []iterator
	for _, t := range m.stack {
		iter, err := t.seekRecord(rec)
		if err != nil {
			return nil, err
		}
		its = append(its, iter)
	}

	merged := &mergedIter{
		typ:   rec.typ(),
		stack: its,
	}

	if err := merged.init(); err != nil {
		return nil, err
	}

	return merged, nil
}

// mergedIter iterates over a stack of reftables. Entries from higher
// in the stack obscure lower entries.
type mergedIter struct {
	// TODO: We could short-circuit the entries coming from the
	// base stack
	typ   byte
	pq    mergedIterPQueue
	stack []iterator
}

func (it *mergedIter) init() error {
	for i, sub := range it.stack {
		rec := newRecord(it.typ, "")
		ok, err := sub.Next(rec)
		if err != nil {
			return err
		}
		if ok {
			it.pq.add(pqEntry{
				rec:   rec,
				index: i,
			})
		} else {
			it.stack[i] = nil
		}
	}

	return nil
}

// advance one iterator, putting its result into the priority queue.
func (m *mergedIter) advanceSubIter(index int) error {
	if m.stack[index] == nil {
		return nil
	}

	r := newRecord(m.typ, "")
	ok, err := m.stack[index].Next(r)
	if err != nil {
		return err
	}

	if !ok {
		m.stack[index] = nil
		return nil
	}

	m.pq.add(pqEntry{r, index})
	return nil
}

func (m *mergedIter) Next(rec record) (bool, error) {
	if m.pq.isEmpty() {
		return false, nil
	}

	entry := m.pq.remove()
	if err := m.advanceSubIter(entry.index); err != nil {
		return err
	}

	for !m.pq.isEmpty() {
		top := m.pq.top()
		if top.rec.key() > entry.rec.key() {
			break
		}

		m.pq.remove()
		if err := m.advanceSubIter(top.index); err != nil {
			return err
		}
	}

	rec.copyFrom(entry.rec)
	return true, nil
}
