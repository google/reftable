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
	"path"
	"sort"
	"strings"
)

func hasRef(tab Table, additions []string, deletions map[string]bool, name string) (bool, error) {
	if idx := sort.SearchStrings(additions, name); idx < len(additions) && additions[idx] == name {
		return true, nil
	}
	if deletions[name] {
		return false, nil
	}
	it, err := tab.SeekRef(name)
	if err != nil {
		return false, err
	}

	var rec RefRecord
	ok, err := it.NextRef(&rec)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	return rec.RefName == name, nil
}

func hasRefWithPrefix(tab Table, additions []string, deletions map[string]bool, prefix string) (bool, error) {
	idx := sort.SearchStrings(additions, prefix)
	if idx < len(additions) && strings.HasPrefix(additions[idx], prefix) {
		return true, nil
	}

	it, err := tab.SeekRef(prefix)
	if err != nil {
		return false, err
	}

	for {
		var rec RefRecord
		ok, err := it.NextRef(&rec)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		if deletions[rec.RefName] {
			continue
		}
		return strings.HasPrefix(rec.RefName, prefix), nil
	}
}

func validateRefname(name string) bool {
	for _, comp := range strings.Split(name, "/") {
		if comp == "." || comp == ".." || comp == "" {
			return false
		}
	}

	return true
}

func validateRefRecordAddition(tab Table, refs []RefRecord) error {
	var additions []string
	deletions := map[string]bool{}
	for _, r := range refs {
		if r.IsDeletion() {
			deletions[r.RefName] = true
		} else {
			additions = append(additions, r.RefName)
		}
	}
	return validateAddition(tab, additions, deletions)
}

func validateAddition(tab Table, additions []string, deletions map[string]bool) error {
	for _, a := range additions {
		if !validateRefname(a) {
			return fmt.Errorf("ref %q has invalid name", a)
		}
		if ok, err := hasRefWithPrefix(tab, additions, deletions, a+"/"); err != nil {
			return err
		} else if ok {
			return fmt.Errorf("reftable: %q is an existing ref prefix", a)
		}

		for a != "" {
			dir, _ := path.Split(a)
			dir = strings.TrimSuffix(dir, "/")
			if ok, err := hasRef(tab, additions, deletions, dir); err != nil {
				return err
			} else if ok {
				return fmt.Errorf("reftable: %q is an existing ref", dir)
			}

			a = dir
		}
	}

	return nil
}
