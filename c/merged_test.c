/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "merged.h"

#include "system.h"

#include "basics.h"
#include "blocksource.h"
#include "constants.h"
#include "pq.h"
#include "reader.h"
#include "record.h"
#include "reftable.h"
#include "test_framework.h"
#include "reftable-tests.h"

static void test_pq(void)
{
	char *names[54] = { NULL };
	int N = ARRAY_SIZE(names) - 1;

	struct merged_iter_pqueue pq = { NULL };
	const char *last = NULL;

	int i = 0;
	for (i = 0; i < N; i++) {
		char name[100];
		snprintf(name, sizeof(name), "%02d", i);
		names[i] = xstrdup(name);
	}

	i = 1;
	do {
		struct reftable_record rec =
			reftable_new_record(BLOCK_TYPE_REF);
		struct pq_entry e = { 0 };

		reftable_record_as_ref(&rec)->refname = names[i];
		e.rec = rec;
		merged_iter_pqueue_add(&pq, e);
		merged_iter_pqueue_check(pq);
		i = (i * 7) % N;
	} while (i != 1);

	while (!merged_iter_pqueue_is_empty(pq)) {
		struct pq_entry e = merged_iter_pqueue_remove(&pq);
		struct reftable_ref_record *ref =
			reftable_record_as_ref(&e.rec);

		merged_iter_pqueue_check(pq);

		if (last != NULL) {
			assert(strcmp(last, ref->refname) < 0);
		}
		last = ref->refname;
		ref->refname = NULL;
		reftable_free(ref);
	}

	for (i = 0; i < N; i++) {
		reftable_free(names[i]);
	}

	merged_iter_pqueue_clear(&pq);
}

static void write_test_table(struct strbuf *buf,
			     struct reftable_ref_record refs[], int n)
{
	int min = 0xffffffff;
	int max = 0;
	int i = 0;
	int err;

	struct reftable_write_options opts = {
		.block_size = 256,
	};
	struct reftable_writer *w = NULL;
	for (i = 0; i < n; i++) {
		uint64_t ui = refs[i].update_index;
		if (ui > max) {
			max = ui;
		}
		if (ui < min) {
			min = ui;
		}
	}

	w = reftable_new_writer(&strbuf_add_void, buf, &opts);
	reftable_writer_set_limits(w, min, max);

	for (i = 0; i < n; i++) {
		uint64_t before = refs[i].update_index;
		int n = reftable_writer_add_ref(w, &refs[i]);
		assert(n == 0);
		assert(before == refs[i].update_index);
	}

	err = reftable_writer_close(w);
	assert_err(err);

	reftable_writer_free(w);
}

static struct reftable_merged_table *
merged_table_from_records(struct reftable_ref_record **refs,
			  struct reftable_block_source **source,
			  struct reftable_reader ***readers, int *sizes,
			  struct strbuf *buf, int n)
{
	int i = 0;
	struct reftable_merged_table *mt = NULL;
	int err;
	struct reftable_table *tabs =
		reftable_calloc(n * sizeof(struct reftable_table));
	*readers = reftable_calloc(n * sizeof(struct reftable_reader *));
	*source = reftable_calloc(n * sizeof(**source));
	for (i = 0; i < n; i++) {
		write_test_table(&buf[i], refs[i], sizes[i]);
		block_source_from_strbuf(&(*source)[i], &buf[i]);

		err = reftable_new_reader(&(*readers)[i], &(*source)[i],
					  "name");
		assert_err(err);
		reftable_table_from_reader(&tabs[i], (*readers)[i]);
	}

	err = reftable_new_merged_table(&mt, tabs, n, SHA1_ID);
	assert_err(err);
	return mt;
}

static void readers_destroy(struct reftable_reader **readers, size_t n)
{
	int i = 0;
	for (; i < n; i++)
		reftable_reader_free(readers[i]);
	reftable_free(readers);
}

static void test_merged_between(void)
{
	uint8_t hash1[SHA1_SIZE] = { 1, 2, 3, 0 };

	struct reftable_ref_record r1[] = { {
		.refname = "b",
		.update_index = 1,
		.value = hash1,
	} };
	struct reftable_ref_record r2[] = { {
		.refname = "a",
		.update_index = 2,
	} };

	struct reftable_ref_record *refs[] = { r1, r2 };
	int sizes[] = { 1, 1 };
	struct strbuf bufs[2] = { STRBUF_INIT, STRBUF_INIT };
	struct reftable_block_source *bs = NULL;
	struct reftable_reader **readers = NULL;
	struct reftable_merged_table *mt =
		merged_table_from_records(refs, &bs, &readers, sizes, bufs, 2);
	int i;
	struct reftable_ref_record ref = { NULL };
	struct reftable_iterator it = { NULL };
	int err = reftable_merged_table_seek_ref(mt, &it, "a");
	assert_err(err);

	err = reftable_iterator_next_ref(&it, &ref);
	assert_err(err);
	assert(ref.update_index == 2);
	reftable_ref_record_clear(&ref);
	reftable_iterator_destroy(&it);
	readers_destroy(readers, 2);
	reftable_merged_table_free(mt);
	for (i = 0; i < ARRAY_SIZE(bufs); i++) {
		strbuf_release(&bufs[i]);
	}
	reftable_free(bs);
}

static void test_merged(void)
{
	uint8_t hash1[SHA1_SIZE] = { 1 };
	uint8_t hash2[SHA1_SIZE] = { 2 };
	struct reftable_ref_record r1[] = { {
						    .refname = "a",
						    .update_index = 1,
						    .value = hash1,
					    },
					    {
						    .refname = "b",
						    .update_index = 1,
						    .value = hash1,
					    },
					    {
						    .refname = "c",
						    .update_index = 1,
						    .value = hash1,
					    } };
	struct reftable_ref_record r2[] = { {
		.refname = "a",
		.update_index = 2,
	} };
	struct reftable_ref_record r3[] = {
		{
			.refname = "c",
			.update_index = 3,
			.value = hash2,
		},
		{
			.refname = "d",
			.update_index = 3,
			.value = hash1,
		},
	};

	struct reftable_ref_record want[] = {
		r2[0],
		r1[1],
		r3[0],
		r3[1],
	};

	struct reftable_ref_record *refs[] = { r1, r2, r3 };
	int sizes[3] = { 3, 1, 2 };
	struct strbuf bufs[3] = { STRBUF_INIT, STRBUF_INIT, STRBUF_INIT };
	struct reftable_block_source *bs = NULL;
	struct reftable_reader **readers = NULL;
	struct reftable_merged_table *mt =
		merged_table_from_records(refs, &bs, &readers, sizes, bufs, 3);

	struct reftable_iterator it = { NULL };
	int err = reftable_merged_table_seek_ref(mt, &it, "a");
	struct reftable_ref_record *out = NULL;
	size_t len = 0;
	size_t cap = 0;
	int i = 0;

	assert_err(err);
	while (len < 100) { /* cap loops/recursion. */
		struct reftable_ref_record ref = { NULL };
		int err = reftable_iterator_next_ref(&it, &ref);
		if (err > 0) {
			break;
		}
		if (len == cap) {
			cap = 2 * cap + 1;
			out = reftable_realloc(
				out, sizeof(struct reftable_ref_record) * cap);
		}
		out[len++] = ref;
	}
	reftable_iterator_destroy(&it);

	assert(ARRAY_SIZE(want) == len);
	for (i = 0; i < len; i++) {
		assert(reftable_ref_record_equal(&want[i], &out[i], SHA1_SIZE));
	}
	for (i = 0; i < len; i++) {
		reftable_ref_record_clear(&out[i]);
	}
	reftable_free(out);

	for (i = 0; i < 3; i++) {
		strbuf_release(&bufs[i]);
	}
	readers_destroy(readers, 3);
	reftable_merged_table_free(mt);
	reftable_free(bs);
}

static void test_default_write_opts(void)
{
	struct reftable_write_options opts = { 0 };
	struct strbuf buf = STRBUF_INIT;
	struct reftable_writer *w =
		reftable_new_writer(&strbuf_add_void, &buf, &opts);

	struct reftable_ref_record rec = {
		.refname = "master",
		.update_index = 1,
	};
	int err;
	struct reftable_block_source source = { NULL };
	struct reftable_table *tab = reftable_calloc(sizeof(*tab) * 1);
	uint32_t hash_id;
	struct reftable_reader *rd = NULL;
	struct reftable_merged_table *merged = NULL;

	reftable_writer_set_limits(w, 1, 1);

	err = reftable_writer_add_ref(w, &rec);
	assert_err(err);

	err = reftable_writer_close(w);
	assert_err(err);
	reftable_writer_free(w);

	block_source_from_strbuf(&source, &buf);

	err = reftable_new_reader(&rd, &source, "filename");
	assert_err(err);

	hash_id = reftable_reader_hash_id(rd);
	assert(hash_id == SHA1_ID);

	reftable_table_from_reader(&tab[0], rd);
	err = reftable_new_merged_table(&merged, tab, 1, SHA1_ID);
	assert_err(err);

	reftable_reader_free(rd);
	reftable_merged_table_free(merged);
	strbuf_release(&buf);
}

/* XXX test refs_for(oid) */

int merged_test_main(int argc, const char *argv[])
{
	add_test_case("test_merged_between", &test_merged_between);
	add_test_case("test_pq", &test_pq);
	add_test_case("test_merged", &test_merged);
	add_test_case("test_default_write_opts", &test_default_write_opts);
	return test_main(argc, argv);
}
