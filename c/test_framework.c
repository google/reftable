/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "test_framework.h"

#include "system.h"
#include "basics.h"

static struct test_case **test_cases;
static int test_case_len;
static int test_case_cap;

static struct test_case *new_test_case(const char *name, void (*testfunc)(void))
{
	struct test_case *tc = reftable_malloc(sizeof(struct test_case));
	tc->name = name;
	tc->testfunc = testfunc;
	return tc;
}

struct test_case *add_test_case(const char *name, void (*testfunc)(void))
{
	struct test_case *tc = new_test_case(name, testfunc);
	if (test_case_len == test_case_cap) {
		test_case_cap = 2 * test_case_cap + 1;
		test_cases = reftable_realloc(
			test_cases, sizeof(struct test_case) * test_case_cap);
	}

	test_cases[test_case_len++] = tc;
	return tc;
}

int test_main(int argc, const char *argv[])
{
	const char *filter = NULL;
	int i = 0;
	if (argc > 1) {
		filter = argv[1];
	}

	for (i = 0; i < test_case_len; i++) {
		const char *name = test_cases[i]->name;
		if (filter == NULL || strstr(name, filter) != NULL) {
			printf("case %s\n", name);
			test_cases[i]->testfunc();
		} else {
			printf("skip %s\n", name);
		}

		reftable_free(test_cases[i]);
	}
	reftable_free(test_cases);
	test_cases = NULL;
	test_case_len = 0;
	test_case_cap = 0;
	return 0;
}

void set_test_hash(uint8_t *p, int i)
{
	memset(p, (uint8_t)i, hash_size(SHA1_ID));
}
