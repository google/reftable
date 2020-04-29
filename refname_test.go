/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

package reftable

import "testing"

func TestValidateRefName(t *testing.T) {
	for in, want := range map[string]bool{
		"a//b":              false,
		"refs/heads/master": true,
		"ab/":               false,
		"ab":                true,
		"":                  false,
		"a/./b":             false,
		"a/../b":            false,
		"a/.../b":           true,
	} {
		got := validateRefname(in)
		if want != got {
			t.Errorf("validateRefname(%q): got %v want %v", in, got, want)
		}
	}
}

func TestHasRef(t *testing.T) {
	var refs []RefRecord
	for _, s := range []string{
		"a",
		"a/b",
		"prefix/a",
		"prefix/b",
	} {
		refs = append(refs, RefRecord{RefName: s})
	}
	_, r := constructTestTable(t,
		refs, nil, Config{})

	add := []string{"a/c", "bb", "prefix/c"}
	del := map[string]bool{
		"a/b":      true,
		"prefix/b": true,
	}

	for nm, want := range map[string]bool{
		"a": true, "aa": false,
		"a/b": false, "a/": false,
		"a/c": true, "bb": true, "prefix/c": true,
		"prefix/b": false, "prefix/a": true,
		"prefix": false,
	} {
		ok, err := hasRef(r, add, del, nm)
		if err != nil {
			t.Errorf("hasRef(%q): %v", nm, err)
		}
		if ok != want {
			t.Errorf("hasRef(%q): got %v want %v", nm, ok, want)
		}
	}

	for nm, want := range map[string]bool{
		"a": true, "aa": false,
		"a/":  true,
		"a/c": true, "b": true, "bb": true,
		"prefix/": true,
	} {
		ok, err := hasRefWithPrefix(r, add, del, nm)
		if err != nil {
			t.Errorf("hasRef(%q): %v", nm, err)
		}
		if ok != want {
			t.Errorf("hasRef(%q): got %v want %v", nm, ok, want)
		}
	}
}

func TestValidateAddition(t *testing.T) {
	type testcase struct {
		add []string
		del []string
		ok  bool
	}

	var refs []RefRecord
	for _, s := range []string{
		"a/b",
		"a/c",
		"prefix/a",
		"prefix/b",
	} {
		refs = append(refs, RefRecord{RefName: s})
	}
	_, r := constructTestTable(t,
		refs, nil, Config{})

	for _, tc := range []testcase{
		{add: []string{"a"}, ok: false},
		{
			add: []string{"a"},
			del: []string{"a/b", "a/c"},
			ok:  true,
		},
		{add: []string{"q", "q/r"}, ok: false},
		{add: []string{"a/b/c"}, ok: false},
		{add: []string{"q//r"}, ok: false},
		{del: []string{"q//r"}, ok: true},
	} {
		asMap := map[string]bool{}
		for _, d := range tc.del {
			asMap[d] = true
		}

		err := validateAddition(r, tc.add, asMap)
		if (err == nil) != tc.ok {
			t.Errorf("case add %v, del %v: got err %v, want ok %v", tc.add, tc.del, err, tc.ok)
		}
	}
}
