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
	char *names[54] = {};
	int N = ARRAYSIZE(names) - 1;

	int i = 0;
	for (i = 0; i < N; i++) {
		char name[100];
		snprintf(name, sizeof(name), "%02d", i);
		names[i] = strdup(name);
	}

	struct merged_iter_pqueue pq = {};

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
		struct ref_record *ref = record_as_ref(e.rec);

		if (last != NULL) {
			assert(strcmp(last, ref->ref_name) < 0);
		}
		last = ref->ref_name;
		ref->ref_name = NULL;
		free(ref);
	}

	for (i = 0; i < N; i++) {
		free(names[i]);
	}

	merged_iter_pqueue_clear(&pq);
}

void write_test_table(struct slice *buf, struct ref_record refs[], int n)
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

	struct write_options opts = {
		.block_size = 256,
	};

	struct writer *w = new_writer(&slice_write_void, buf, &opts);
	writer_set_limits(w, min, max);

	for (i = 0; i < n; i++) {
		uint64_t before = refs[i].update_index;
		int n = writer_add_ref(w, &refs[i]);
		assert(n == 0);
		assert(before == refs[i].update_index);
	}

	int err = writer_close(w);
	assert(err == 0);

	writer_free(w);
	w = NULL;
}

static struct merged_table *merged_table_from_records(struct ref_record **refs,
						      int *sizes,
						      struct slice *buf, int n)
{
	struct block_source *source = calloc(n, sizeof(*source));
	struct reader **rd = calloc(n, sizeof(*rd));
	int i = 0;
	for (i = 0; i < n; i++) {
		write_test_table(&buf[i], refs[i], sizes[i]);
		block_source_from_slice(&source[i], &buf[i]);

		int err = new_reader(&rd[i], source[i], "name");
		assert(err == 0);
	}

	struct merged_table *mt = NULL;
	int err = new_merged_table(&mt, rd, n);
	assert(err == 0);
	return mt;
}

void test_merged_between(void)
{
	byte hash1[SHA1_SIZE];
	byte hash2[SHA1_SIZE];

	set_test_hash(hash1, 1);
	set_test_hash(hash2, 2);
	struct ref_record r1[] = { {
		.ref_name = "b",
		.update_index = 1,
		.value = hash1,
	} };
	struct ref_record r2[] = { {
		.ref_name = "a",
		.update_index = 2,
	} };

	struct ref_record *refs[] = { r1, r2 };
	int sizes[] = { 1, 1 };
	struct slice bufs[2] = {};
	struct merged_table *mt =
		merged_table_from_records(refs, sizes, bufs, 2);

	struct iterator it = {};
	int err = merged_table_seek_ref(mt, &it, "a");
	assert(err == 0);

	struct ref_record ref = {};
	err = iterator_next_ref(it, &ref);
	assert_err(err);
	assert(ref.update_index == 2);
}

void test_merged(void)
{
	byte hash1[SHA1_SIZE];
	byte hash2[SHA1_SIZE];

	set_test_hash(hash1, 1);
	set_test_hash(hash2, 2);
	struct ref_record r1[] = { {
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
	struct ref_record r2[] = { {
		.ref_name = "a",
		.update_index = 2,
	} };
	struct ref_record r3[] = {
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

	struct ref_record *refs[] = { r1, r2, r3 };
	int sizes[3] = { 3, 1, 2 };
	struct slice bufs[3] = {};

	struct merged_table *mt =
		merged_table_from_records(refs, sizes, bufs, 3);

	struct iterator it = {};
	int err = merged_table_seek_ref(mt, &it, "a");
	assert(err == 0);

	struct ref_record *out = NULL;
	int len = 0;
	int cap = 0;
	while (len < 100) { /* cap loops/recursion. */
		struct ref_record ref = {};
		int err = iterator_next_ref(it, &ref);
		if (err > 0) {
			break;
		}
		if (len == cap) {
			cap = 2 * cap + 1;
			out = realloc(out, sizeof(struct ref_record) * cap);
		}
		out[len++] = ref;
	}
	iterator_destroy(&it);

	struct ref_record want[] = {
		r2[0],
		r1[1],
		r3[0],
		r3[1],
	};
	assert(ARRAYSIZE(want) == len);
	int i = 0;
	for (i = 0; i < len; i++) {
		assert(ref_record_equal(&want[i], &out[i], SHA1_SIZE));
	}
	for (i = 0; i < len; i++) {
		ref_record_clear(&out[i]);
	}
	free(out);

	for (i = 0; i < 3; i++) {
		free(slice_yield(&bufs[i]));
	}
	merged_table_close(mt);
	merged_table_free(mt);
}

/* XXX test refs_for(oid) */

int main()
{
	add_test_case("test_merged_between", &test_merged_between);
	add_test_case("test_pq", &test_pq);
	add_test_case("test_merged", &test_merged);
	test_main();
}
