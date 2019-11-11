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

// BlockSource is an interface for reading reftable bytes.
type BlockSource interface {
	Size() uint64
	ReadBlock(off uint64, size int) ([]byte, error)
	Close() error
}

// record is a single piece of keyed data, stored in the reftable.
type record interface {
	Key() string
	Type() byte
	CopyFrom(record)
	IsTombstone() bool
	String() string

	valType() uint8
	encode(buf []byte) (n int, fits bool)
	decode(buf []byte, key string, valType uint8) (n int, ok bool)
}

// Table is a read interface for reftables, either file reftables or merged reftables.
type Table interface {
	MaxUpdateIndex() uint64
	MinUpdateIndex() uint64
	SeekRef(ref *RefRecord) (*Iterator, error)
	SeekLog(ref *LogRecord) (*Iterator, error)
	RefsFor(oid []byte) (*Iterator, error)
}

// iterator is an iterator over reftable
type Iterator struct {
	impl iterator
}

// Options define write options for reftables.
type Options struct {
	// If set,  do not pad blocks to blocksize.
	Unpadded bool

	// The block size, if not set 4096.
	BlockSize        uint32
	MinUpdateIndex   uint64
	MaxUpdateIndex   uint64
	SkipIndexObjects bool
	RestartInterval  int
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
	RefName  string
	TS       uint64
	New      []byte
	Old      []byte
	Name     string
	Email    string
	Time     uint64
	TZOffset int16
	Message  string
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
