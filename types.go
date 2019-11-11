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

// header is the file header present both at start and end of file.
type header struct {
	Magic          [4]byte
	BlockSize      uint32
	MinUpdateIndex uint64
	MaxUpdateIndex uint64
}

// footer is the file footer present only at the end of file.
type footer struct {
	// Footer lacks RefOffset, because it is always 24 (if present)
	RefIndexOffset uint64

	// On serialization, offset is <<5, lower bits hold id size
	ObjOffset      uint64
	ObjIndexOffset uint64
	LogOffset      uint64
	LogIndexOffset uint64
}
