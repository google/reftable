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
#include "reftable-tests.h"

static void test_slice(void)
{
	struct slice s = SLICE_INIT;
	struct slice t = SLICE_INIT;

	slice_addstr(&s, "abc");
	assert(0 == strcmp("abc", slice_as_string(&s)));

	slice_addstr(&t, "pqr");
	slice_addbuf(&s, &t);
	assert(0 == strcmp("abcpqr", slice_as_string(&s)));

	slice_release(&s);
	slice_release(&t);
}

int slice_test_main(int argc, const char *argv[])
{
	add_test_case("test_slice", &test_slice);
	return test_main(argc, argv);
}
