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

import "bytes"

type emptyIterator struct {
}

func (e *emptyIterator) Next(Record) (bool, error) {
	return false, nil
}

// filteringRefIterator applies filtering for `oid` to the block
type filteringRefIterator struct {
	// The object ID to filter for
	oid []byte

	// doubleCheck if set, will cause the refs to be checked
	// against the table in `tab`.
	doubleCheck bool
	tab         Table

	// mutable
	it Iterator
}

// Next implements the Iterator interface.
func (it *filteringRefIterator) Next(rec Record) (bool, error) {
	ref := rec.(*RefRecord)
	for {
		ok, err := it.it.Next(ref)
		if !ok || err != nil {
			return false, err
		}

		if it.doubleCheck {
			it, err := it.tab.Seek(ref)
			if err != nil {
				return false, err
			}

			ok, err := it.Next(ref)
			if !ok || err != nil {
				return false, err
			}
		}

		if bytes.Compare(ref.Value, it.oid) == 0 || bytes.Compare(ref.TargetValue, it.oid) == 0 {
			return true, err
		}
	}
}
