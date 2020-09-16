/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "strbuf.h"

#include "system.h"

#include "basics.h"
#include "test_framework.h"
#include "reftable-tests.h"

static void test_strbuf(void)
{
	struct strbuf s = STRBUF_INIT;
	struct strbuf t = STRBUF_INIT;

	strbuf_addstr(&s, "abc");
	assert(0 == strcmp("abc", s.buf));

	strbuf_addstr(&t, "pqr");
	strbuf_addbuf(&s, &t);
	assert(0 == strcmp("abcpqr", s.buf));

	strbuf_release(&s);
	strbuf_release(&t);
}

int strbuf_test_main(int argc, const char *argv[])
{
	add_test_case("test_strbuf", &test_strbuf);
	return test_main(argc, argv);
}
