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
	struct slice s = { 0 };
	slice_set_string(&s, "abc");
	assert(0 == strcmp("abc", slice_as_string(&s)));

	struct slice t = { 0 };
	slice_set_string(&t, "pqr");

	slice_append(&s, t);
	assert(0 == strcmp("abcpqr", slice_as_string(&s)));

	slice_release(&s);
	slice_release(&t);
}

int main(int argc, char *argv[])
{
	add_test_case("test_slice", &test_slice);
	test_main(argc, argv);
}
