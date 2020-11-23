/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "test_framework.h"

#include "system.h"
#include "basics.h"

void set_test_hash(uint8_t *p, int i)
{
	memset(p, (uint8_t)i, hash_size(SHA1_ID));
}
