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

var magic = [4]byte{'R', 'E', 'F', 'T'}

const hashSize = 20
const version = 1
const headerSize = 24
const footerSize = 68

const BlockTypeLog = 'g'
const BlockTypeIndex = 'i'
const BlockTypeRef = 'r'
const BlockTypeObj = 'o'
const blockTypeAny = 0

const maxRestarts = (1 << 16) - 1
