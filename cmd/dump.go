/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/google/reftable"
)

func main() {
	optDumpTab := flag.Bool("table", false, "dump single reftable")
	optDumpStack := flag.Bool("stack", true, "dump a stack of reftables")
	flag.Parse()

	if len(flag.Args()) != 1 {
		log.Fatalf("Need 1 argument.")
	}

	arg := flag.Arg(0)
	if *optDumpTab {
		if err := dumpTableFile(arg); err != nil {
			log.Fatalf("dumpTableFile(%s): %v", arg, err)
		}
	} else if *optDumpStack {
		if err := dumpStack(arg); err != nil {
			log.Fatal(err)
		}
	} else if _, err := os.Lstat(".git"); err == nil {
		if err := dumpStack(".git/reftable"); err != nil {
			log.Fatal(err)
		}
	}
}

func dumpStack(dir string) error {
	st, err := reftable.NewStack(dir, reftable.Config{})
	if err != nil {
		return err
	}
	defer st.Close()

	merged := st.Merged()
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

	fmt.Printf("** DEBUG **\n%s\n", r.DebugData())

	return dumpTable(r)
}

func dumpTable(tab reftable.Table) error {
	iter, err := tab.SeekRef("")
	if err != nil {
		return err
	}

	fmt.Printf("** REFS **\n")

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
