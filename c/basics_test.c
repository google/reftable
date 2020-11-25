/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "system.h"

#include "basics.h"
#include "test_framework.h"
#include "reftable-tests.h"

struct binsearch_args {
	int key;
	int *arr;
};

static int binsearch_func(size_t i, void *void_args)
{
	struct binsearch_args *args = (struct binsearch_args *)void_args;

	return args->key < args->arr[i];
}

static void test_binsearch(void)
{
	int arr[] = { 2, 4, 6, 8, 10 };
	size_t sz = ARRAY_SIZE(arr);
	struct binsearch_args args = {
		.arr = arr,
	};

	int i = 0;
	for (i = 1; i < 11; i++) {
		int res;
		args.key = i;
		res = binsearch(sz, &binsearch_func, &args);

		if (res < sz) {
			EXPECT(args.key < arr[res]);
			if (res > 0) {
				EXPECT(args.key >= arr[res - 1]);
			}
		} else {
			EXPECT(args.key == 10 || args.key == 11);
		}
	}
}

int basics_test_main(int argc, const char *argv[])
{
	test_binsearch();
	return 0;
}
