/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "test_framework.h"

#include "system.h"
#include "basics.h"
#include "constants.h"

struct test_case **test_cases;
int test_case_len;
int test_case_cap;

struct test_case *new_test_case(const char *name, void (*testfunc)())
{
	struct test_case *tc = reftable_malloc(sizeof(struct test_case));
	tc->name = name;
	tc->testfunc = testfunc;
	return tc;
}

struct test_case *add_test_case(const char *name, void (*testfunc)())
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

void test_main()
{
	int i = 0;
	for (i = 0; i < test_case_len; i++) {
		printf("case %s\n", test_cases[i]->name);
		test_cases[i]->testfunc();
	}
}

void set_test_hash(byte *p, int i)
{
	memset(p, (byte)i, hash_size(SHA1_ID));
}

void print_names(char **a)
{
	if (a == NULL || *a == NULL) {
		puts("[]");
		return;
	}
	puts("[");
	char **p = a;
	while (*p) {
		puts(*p);
		p++;
	}
	puts("]");
}
