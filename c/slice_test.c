/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "slice.h"

#include "system.h"

#include "basics.h"
#include "record.h"
#include "reftable.h"
#include "test_framework.h"

void test_slice(void)
{
	struct slice s = {};
	slice_set_string(&s, "abc");
	assert(0 == strcmp("abc", slice_as_string(&s)));

	struct slice t = {};
	slice_set_string(&t, "pqr");

	slice_append(&s, t);
	assert(0 == strcmp("abcpqr", slice_as_string(&s)));

	free(slice_yield(&s));
	free(slice_yield(&t));
}

int main()
{
	add_test_case("test_slice", &test_slice);
	test_main();
}
