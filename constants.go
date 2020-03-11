/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

package reftable

var magic = [4]byte{'R', 'E', 'F', 'T'}

func headerSize(version int) int {
	if version == 1 {
		return 24

	}
	return 28
}

func footerSize(version int) int {
	if version == 1 {
		return 68
	}

	return 72
}

const defaultBlockSize = 4096

const blockTypeLog = 'g'
const blockTypeIndex = 'i'
const blockTypeRef = 'r'
const blockTypeObj = 'o'
const blockTypeAny = 0

const maxRestarts = (1 << 16) - 1
