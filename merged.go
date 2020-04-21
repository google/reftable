/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

package reftable

import (
	"fmt"
	"log"
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
	stack  []*Reader
	hashID HashID

	suppressDeletions bool
}

func (m *Merged) HashID() HashID {
	return m.hashID
}

// NewMerged creates a reader for a merged reftable.
func NewMerged(tabs []*Reader, hashID [4]byte) (*Merged, error) {
	var last Table
	for i, t := range tabs {
		if last != nil && last.MaxUpdateIndex() >= t.MinUpdateIndex() {
			return nil, fmt.Errorf("reftable: table %d has min %d, table %d has max %d; indices must be increasing.", i, t.MinUpdateIndex(), i-1, last.MaxUpdateIndex())
		}
		if t.HashID() != hashID {
			return nil, fmt.Errorf("reftable: table %d has hash ID %q want hash ID %q", i,
				t.HashID(), hashID)
		}
		last = t
	}

	return &Merged{
		stack:  tabs,
		hashID: hashID,
	}, nil
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

// Seek returns an iterator positioned before the wanted record.
func (m *Merged) SeekLog(refname string, updateIndex uint64) (*Iterator, error) {
	log := LogRecord{
		RefName:     refname,
		UpdateIndex: updateIndex,
	}
	impl, err := m.seek(&log)
	if err != nil {
		return nil, err
	}
	return &Iterator{impl}, nil
}

func (m *Merged) SeekRef(name string) (*Iterator, error) {
	ref := RefRecord{
		RefName: name,
	}
	impl, err := m.seek(&ref)
	if err != nil {
		return nil, err
	}
	return &Iterator{impl}, nil
}

func (m *Merged) seek(rec record) (iterator, error) {
	var its []iterator
	var names []string
	for _, t := range m.stack {
		iter, err := t.seekRecord(rec)
		if err != nil {
			return nil, fmt.Errorf("reftable: seek %s: %v", t.Name(), err)
		}
		its = append(its, iter)
		names = append(names, t.Name())
	}

	merged := &mergedIter{
		typ:               rec.typ(),
		suppressDeletions: m.suppressDeletions,
		stack:             its,
		names:             names,
	}

	if err := merged.init(); err != nil {
		return nil, err
	}

	return merged, nil
}

// in the stack obscure lower entries.
type mergedIter struct {
	// TODO: We could short-circuit the entries coming from the
	// base stack
	typ byte
	pq  mergedIterPQueue

	suppressDeletions bool

	// Fixed side arrays, matched with merged.stack
	stack []iterator
	names []string
}

func (it *mergedIter) init() error {
	for i, sub := range it.stack {
		rec := newRecord(it.typ, "")
		ok, err := sub.Next(rec)
		if err != nil {
			return fmt.Errorf("init %s: %v", it.names[i], err)
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
		return fmt.Errorf("next %s: %v", m.names[index], err)
	}

	if !ok {
		m.stack[index] = nil
		return nil
	}

	m.pq.add(pqEntry{r, index})
	return nil
}

func (m *mergedIter) Next(rec record) (bool, error) {
	for {
		ok, err := m.nextEntry(rec)
		if ok && rec.IsDeletion() && m.suppressDeletions {
			continue
		}

		return ok, err
	}
}

func (m *mergedIter) nextEntry(rec record) (bool, error) {
	if m.pq.isEmpty() {
		return false, nil
	}

	entry := m.pq.remove()
	if err := m.advanceSubIter(entry.index); err != nil {
		return false, err
	}

	// One can also use reftable as datacenter-local storage, where the
	// ref database is maintained in globally consistent database
	// (eg. CockroachDB or Spanner). In this scenario, replication
	// delays together with compaction may cause newer tables to
	// contain older entries. In such a deployment, the loop below
	// must be changed to collect all entries for the same key,
	// and return new the newest one.
	for !m.pq.isEmpty() {
		top := m.pq.top()
		if top.rec.key() > entry.rec.key() {
			break
		}

		m.pq.remove()
		if err := m.advanceSubIter(top.index); err != nil {
			return false, err
		}
	}

	rec.copyFrom(entry.rec)
	return true, nil
}
