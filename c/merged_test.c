/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "merged.h"

#include "system.h"

#include "basics.h"
#include "block.h"
#include "constants.h"
#include "pq.h"
#include "reader.h"
#include "record.h"
#include "reftable.h"
#include "test_framework.h"

void test_pq(void)
{
	char *names[54] = { 0 };
	int N = ARRAY_SIZE(names) - 1;

	int i = 0;
	for (i = 0; i < N; i++) {
		char name[100];
		snprintf(name, sizeof(name), "%02d", i);
		names[i] = xstrdup(name);
	}

	struct merged_iter_pqueue pq = { 0 };

	i = 1;
	do {
		struct record rec = new_record(BLOCK_TYPE_REF);
		record_as_ref(rec)->ref_name = names[i];

		struct pq_entry e = {
			.rec = rec,
		};
		merged_iter_pqueue_add(&pq, e);
		merged_iter_pqueue_check(pq);
		i = (i * 7) % N;
	} while (i != 1);

	const char *last = NULL;
	while (!merged_iter_pqueue_is_empty(pq)) {
		struct pq_entry e = merged_iter_pqueue_remove(&pq);
		merged_iter_pqueue_check(pq);
		struct reftable_ref_record *ref = record_as_ref(e.rec);

		if (last != NULL) {
			assert(strcmp(last, ref->ref_name) < 0);
		}
		last = ref->ref_name;
		ref->ref_name = NULL;
		reftable_free(ref);
	}

	for (i = 0; i < N; i++) {
		reftable_free(names[i]);
	}

	merged_iter_pqueue_clear(&pq);
}

void write_test_table(struct slice *buf, struct reftable_ref_record refs[],
		      int n)
{
	int min = 0xffffffff;
	int max = 0;
	int i = 0;
	for (i = 0; i < n; i++) {
		uint64_t ui = refs[i].update_index;
		if (ui > max) {
			max = ui;
		}
		if (ui < min) {
			min = ui;
		}
	}

	struct reftable_write_options opts = {
		.block_size = 256,
	};

	struct reftable_writer *w =
		reftable_new_writer(&slice_write_void, buf, &opts);
	reftable_writer_set_limits(w, min, max);

	for (i = 0; i < n; i++) {
		uint64_t before = refs[i].update_index;
		int n = reftable_writer_add_ref(w, &refs[i]);
		assert(n == 0);
		assert(before == refs[i].update_index);
	}

	int err = reftable_writer_close(w);
	assert_err(err);

	reftable_writer_free(w);
}

static struct reftable_merged_table *
merged_table_from_records(struct reftable_ref_record **refs,
			  struct reftable_block_source **source, int *sizes,
			  struct slice *buf, int n)
{
	*source = reftable_calloc(n * sizeof(**source));
	struct reftable_reader **rd = reftable_calloc(n * sizeof(*rd));
	int i = 0;
	for (i = 0; i < n; i++) {
		write_test_table(&buf[i], refs[i], sizes[i]);
		block_source_from_slice(&(*source)[i], &buf[i]);

		int err = reftable_new_reader(&rd[i], (*source)[i], "name");
		assert_err(err);
	}

	struct reftable_merged_table *mt = NULL;
	int err = reftable_new_merged_table(&mt, rd, n, SHA1_ID);
	assert_err(err);
	return mt;
}

void test_merged_between(void)
{
	byte hash1[SHA1_SIZE];
	byte hash2[SHA1_SIZE];

	set_test_hash(hash1, 1);
	set_test_hash(hash2, 2);
	struct reftable_ref_record r1[] = { {
		.ref_name = "b",
		.update_index = 1,
		.value = hash1,
	} };
	struct reftable_ref_record r2[] = { {
		.ref_name = "a",
		.update_index = 2,
	} };

	struct reftable_ref_record *refs[] = { r1, r2 };
	int sizes[] = { 1, 1 };
	struct slice bufs[2] = { 0 };
	struct reftable_block_source *bs = NULL;
	struct reftable_merged_table *mt =
		merged_table_from_records(refs, &bs, sizes, bufs, 2);

	struct reftable_iterator it = { 0 };
	int err = reftable_merged_table_seek_ref(mt, &it, "a");
	assert_err(err);

	struct reftable_ref_record ref = { 0 };
	err = reftable_iterator_next_ref(it, &ref);
	assert_err(err);
	assert(ref.update_index == 2);
	reftable_ref_record_clear(&ref);

	reftable_iterator_destroy(&it);
	reftable_merged_table_close(mt);
	reftable_merged_table_free(mt);
	for (int i = 0; i < ARRAY_SIZE(bufs); i++) {
		slice_clear(&bufs[i]);
	}
	reftable_free(bs);
}

void test_merged(void)
{
	byte hash1[SHA1_SIZE];
	byte hash2[SHA1_SIZE];

	set_test_hash(hash1, 1);
	set_test_hash(hash2, 2);
	struct reftable_ref_record r1[] = { {
						    .ref_name = "a",
						    .update_index = 1,
						    .value = hash1,
					    },
					    {
						    .ref_name = "b",
						    .update_index = 1,
						    .value = hash1,
					    },
					    {
						    .ref_name = "c",
						    .update_index = 1,
						    .value = hash1,
					    } };
	struct reftable_ref_record r2[] = { {
		.ref_name = "a",
		.update_index = 2,
	} };
	struct reftable_ref_record r3[] = {
		{
			.ref_name = "c",
			.update_index = 3,
			.value = hash2,
		},
		{
			.ref_name = "d",
			.update_index = 3,
			.value = hash1,
		},
	};

	struct reftable_ref_record *refs[] = { r1, r2, r3 };
	int sizes[3] = { 3, 1, 2 };
	struct slice bufs[3] = { 0 };
	struct reftable_block_source *bs = NULL;

	struct reftable_merged_table *mt =
		merged_table_from_records(refs, &bs, sizes, bufs, 3);

	struct reftable_iterator it = { 0 };
	int err = reftable_merged_table_seek_ref(mt, &it, "a");
	assert_err(err);

	struct reftable_ref_record *out = NULL;
	int len = 0;
	int cap = 0;
	while (len < 100) { /* cap loops/recursion. */
		struct reftable_ref_record ref = { 0 };
		int err = reftable_iterator_next_ref(it, &ref);
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

	struct reftable_ref_record want[] = {
		r2[0],
		r1[1],
		r3[0],
		r3[1],
	};
	assert(ARRAY_SIZE(want) == len);
	int i = 0;
	for (i = 0; i < len; i++) {
		assert(reftable_ref_record_equal(&want[i], &out[i], SHA1_SIZE));
	}
	for (i = 0; i < len; i++) {
		reftable_ref_record_clear(&out[i]);
	}
	reftable_free(out);

	for (i = 0; i < 3; i++) {
		slice_clear(&bufs[i]);
	}
	reftable_merged_table_close(mt);
	reftable_merged_table_free(mt);
	reftable_free(bs);
}

/* XXX test refs_for(oid) */

int main(int argc, char *argv[])
{
	add_test_case("test_merged_between", &test_merged_between);
	add_test_case("test_pq", &test_pq);
	add_test_case("test_merged", &test_merged);
	test_main(argc, argv);
}
