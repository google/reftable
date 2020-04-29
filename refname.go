/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

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
