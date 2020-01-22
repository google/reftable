/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/google/reftable"
)

func main() {
	tabName := flag.String("table", "", "dump single reftable")
	stackName := flag.String("stack", "", "dump a stack of reftables")
	tabDir := flag.String("table_dir", "", "directory with reftables")
	flag.Parse()

	if *tabName != "" {
		if err := dumpTableFile(*tabName); err != nil {
			log.Fatalf("dumpTableFile(%s): %v", *tabName, err)
		}
	}

	if *stackName != "" {
		if err := dumpStack(*stackName, *tabDir); err != nil {
			log.Fatal(err)
		}
	}
}

func dumpStack(nm, dir string) error {
	f, err := os.Open(nm)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	var names []string
	for scanner.Scan() {
		names = append(names, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return (err)
	}

	var tabs []*reftable.Reader
	for _, nm := range names {
		f, err := reftable.NewFileBlockSource(filepath.Join(dir, nm))
		if err != nil {
			return err
		}
		defer f.Close()
		r, err := reftable.NewReader(f, nm)
		if err != nil {
			return err
		}
		tabs = append(tabs, r)
	}

	merged, err := reftable.NewMerged(tabs)
	if err != nil {
		return err
	}

	return dumpTable(merged)

}

func dumpTableFile(nm string) error {
	f, err := reftable.NewFileBlockSource(nm)
	if err != nil {
		return err
	}

	r, err := reftable.NewReader(f, nm)
	if err != nil {
		return err
	}

	return dumpTable(r)

}

func dumpTable(tab reftable.Table) error {
	iter, err := tab.SeekRef("")
	if err != nil {
		return err
	}

	for {
		var rec reftable.RefRecord
		ok, err := iter.NextRef(&rec)
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		fmt.Printf("%#v\n", rec)
	}

	fmt.Printf("** LOGS **\n")

	var neg int64
	neg = -1
	iter, err = tab.SeekLog("", uint64(neg))
	if err != nil {
		return err
	}

	for {
		var rec reftable.LogRecord
		ok, err := iter.NextLog(&rec)
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		fmt.Printf("%#v\n", rec)
	}
	return nil
}
