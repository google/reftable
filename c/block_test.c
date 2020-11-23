/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "block.h"

#include "system.h"

#include "blocksource.h"
#include "basics.h"
#include "constants.h"
#include "record.h"
#include "reftable.h"
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

static void test_block_read_write(void)
{
	const int header_off = 21; /* random */
	char *names[30];
	const int N = ARRAY_SIZE(names);
	const int block_size = 1024;
	struct reftable_block block = { NULL };
	struct block_writer bw = {
		.last_key = STRBUF_INIT,
	};
	struct reftable_ref_record ref = { NULL };
	struct reftable_record rec = { NULL };
	int i = 0;
	int n;
	struct block_reader br = { 0 };
	struct block_iter it = { .last_key = STRBUF_INIT };
	int j = 0;
	struct strbuf want = STRBUF_INIT;

	block.data = reftable_calloc(block_size);
	block.len = block_size;
	block.source = malloc_block_source();
	block_writer_init(&bw, BLOCK_TYPE_REF, block.data, block_size,
			  header_off, hash_size(SHA1_ID));
	reftable_record_from_ref(&rec, &ref);

	for (i = 0; i < N; i++) {
		char name[100];
		uint8_t hash[SHA1_SIZE];
		snprintf(name, sizeof(name), "branch%02d", i);
		memset(hash, i, sizeof(hash));

		ref.refname = name;
		ref.value = hash;
		names[i] = xstrdup(name);
		n = block_writer_add(&bw, &rec);
		ref.refname = NULL;
		ref.value = NULL;
		EXPECT(n == 0);
	}

	n = block_writer_finish(&bw);
	EXPECT(n > 0);

	block_writer_release(&bw);

	block_reader_init(&br, &block, header_off, block_size, SHA1_SIZE);

	block_reader_start(&br, &it);

	while (1) {
		int r = block_iter_next(&it, &rec);
		EXPECT(r >= 0);
		if (r > 0) {
			break;
		}
		EXPECT_STREQ(names[j], ref.refname);
		j++;
	}

	reftable_record_release(&rec);
	block_iter_close(&it);

	for (i = 0; i < N; i++) {
		struct block_iter it = { .last_key = STRBUF_INIT };
		strbuf_reset(&want);
		strbuf_addstr(&want, names[i]);

		n = block_reader_seek(&br, &it, &want);
		EXPECT(n == 0);

		n = block_iter_next(&it, &rec);
		EXPECT(n == 0);

		EXPECT_STREQ(names[i], ref.refname);

		want.len--;
		n = block_reader_seek(&br, &it, &want);
		EXPECT(n == 0);

		n = block_iter_next(&it, &rec);
		EXPECT(n == 0);
		EXPECT_STREQ(names[10 * (i / 10)], ref.refname);

		block_iter_close(&it);
	}

	reftable_record_release(&rec);
	reftable_block_done(&br.block);
	strbuf_release(&want);
	for (i = 0; i < N; i++) {
		reftable_free(names[i]);
	}
}

int block_test_main(int argc, const char *argv[])
{
	test_binsearch();
	test_block_read_write();
	return 0;
}
