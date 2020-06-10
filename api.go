/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

package reftable

import "errors"

// BlockSource is an interface for reading reftable bytes.
type BlockSource interface {
	Size() uint64
	ReadBlock(off uint64, size int) ([]byte, error)
	Close() error
}

// record is a single piece of keyed data, stored in the reftable.
type record interface {
	key() string
	typ() byte
	copyFrom(record)
	String() string

	valType() uint8
	// XXX negative n == error.s
	encode(buf []byte, hashSize int) (n int, fits bool)
	decode(buf []byte, key string, valType uint8, hashSize int) (n int, ok bool)
	IsDeletion() bool
}

type HashID [4]byte

var SHA1ID = HashID([4]byte{'s', 'h', 'a', '1'})
var SHA256ID = HashID([4]byte{'s', '2', '5', '6'})
var NullHashID = HashID([4]byte{0, 0, 0, 0})

func (i HashID) Size() int {
	switch i {
	case NullHashID, SHA1ID:
		return 20
	case SHA256ID:
		return 32
	}
	panic("unknown hash")
}

// Table is a read interface for reftables, either file reftables or merged reftables.
type Table interface {
	MaxUpdateIndex() uint64
	MinUpdateIndex() uint64
	HashID() HashID
	SeekRef(refName string) (*Iterator, error)
	SeekLog(refName string, updateIndex uint64) (*Iterator, error)
	RefsFor(oid []byte) (*Iterator, error)
}

// iterator is an iterator over reftable
type Iterator struct {
	impl iterator
}

type LogExpirationConfig struct {
	Time           uint64
	MaxUpdateIndex uint64
	MinUpdateIndex uint64
}

// Options define write options for reftables.
type Config struct {
	// If set, do not pad blocks to blocksize.
	Unaligned bool

	// The block size, if not set 4096.
	BlockSize        uint32
	SkipIndexObjects bool
	RestartInterval  int

	// Hash identifier. Unset means sha1.
	HashID HashID

	// Allow dir/file conflicts and illegal refnames
	SkipNameCheck bool

	// If set, store reflog messages exactly. If unset, only allow
	// a single line, and a trailing '\n' is added if it is missing.
	ExactLogMessage bool
}

// RefRecord is a Record from the ref database.
type RefRecord struct {
	RefName     string
	UpdateIndex uint64
	Value       []byte
	TargetValue []byte
	// is a 0-length target allowed?
	Target string
}

// LogRecord is a Record from the reflog database.
type LogRecord struct {
	RefName     string
	UpdateIndex uint64
	New         []byte
	Old         []byte
	Name        string
	Email       string
	Time        uint64
	TZOffset    int16
	Message     string
}

// BlockStats provides write statistics data of a certain block type.
type BlockStats struct {
	Entries       int
	Restarts      int
	Blocks        int
	IndexBlocks   int
	MaxIndexLevel int

	Offset      uint64
	IndexOffset uint64
}

// Stats provides general write statistics
type Stats struct {
	ObjStats BlockStats
	RefStats BlockStats
	LogStats BlockStats
	idxStats BlockStats

	Blocks int

	ObjectIDLen int
}

// ErrEmptyTable indicates that a writer tried to create a table
// without blocks.
var ErrEmptyTable = errors.New("reftable: table is empty")
