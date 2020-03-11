/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

package reftable

// header is the file header present both at start and end of file.
type header struct {
	Magic          [4]byte
	BlockSize      uint32
	MinUpdateIndex uint64
	MaxUpdateIndex uint64
	HashID         HashID
}

// footer is the file footer present only at the end of file.
type footer struct {
	// Footer lacks RefOffset, because it is always headerSize (if present)
	RefIndexOffset uint64

	// On serialization, offset is <<5, lower bits hold id size
	ObjOffset      uint64
	ObjIndexOffset uint64
	LogOffset      uint64
	LogIndexOffset uint64
}
