/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "stack.h"

#include "system.h"

#include "basics.h"
#include "constants.h"
#include "record.h"
#include "reftable.h"
#include "test_framework.h"

void test_read_file(void)
{
	char fn[256] = "/tmp/stack.test_read_file.XXXXXX";
	int fd = mkstemp(fn);
	assert(fd > 0);

	char out[1024] = "line1\n\nline2\nline3";

	int n = write(fd, out, strlen(out));
	assert(n == strlen(out));
	int err = close(fd);
	assert(err >= 0);

	char **names = NULL;
	err = read_lines(fn, &names);
	assert_err(err);

	char *want[] = { "line1", "line2", "line3" };
	int i = 0;
	for (i = 0; names[i] != NULL; i++) {
		assert(0 == strcmp(want[i], names[i]));
	}
	free_names(names);
	remove(fn);
}

void test_parse_names(void)
{
	char buf[] = "line\n";
	char **names = NULL;
	parse_names(buf, strlen(buf), &names);

	assert(NULL != names[0]);
	assert(0 == strcmp(names[0], "line"));
	assert(NULL == names[1]);
	free_names(names);
}

void test_names_equal(void)
{
	char *a[] = { "a", "b", "c", NULL };
	char *b[] = { "a", "b", "d", NULL };
	char *c[] = { "a", "b", NULL };

	assert(names_equal(a, a));
	assert(!names_equal(a, b));
	assert(!names_equal(a, c));
}

int write_test_ref(struct reftable_writer *wr, void *arg)
{
	struct reftable_ref_record *ref = arg;

	reftable_writer_set_limits(wr, ref->update_index, ref->update_index);
	int err = reftable_writer_add_ref(wr, ref);

	return err;
}

int write_test_log(struct reftable_writer *wr, void *arg)
{
	struct reftable_log_record *log = arg;

	reftable_writer_set_limits(wr, log->update_index, log->update_index);
	int err = reftable_writer_add_log(wr, log);

	return err;
}

void test_reftable_stack_add_one(void)
{
	char dir[256] = "/tmp/stack.test_reftable_stack_add.XXXXXX";
	assert(mkdtemp(dir));

	struct reftable_write_options cfg = { 0 };
	struct reftable_stack *st = NULL;
	int err = reftable_new_stack(&st, dir, cfg);
	assert_err(err);

	struct reftable_ref_record ref = {
		.ref_name = "HEAD",
		.update_index = 1,
		.target = "master",
	};

	err = reftable_stack_add(st, &write_test_ref, &ref);
	assert_err(err);

	struct reftable_ref_record dest = { 0 };
	err = reftable_stack_read_ref(st, ref.ref_name, &dest);
	assert_err(err);
	assert(0 == strcmp("master", dest.target));

	reftable_stack_destroy(st);
}

void test_reftable_stack_add(void)
{
	int i = 0;
	char dir[256] = "/tmp/stack.test_reftable_stack_add.XXXXXX";
	assert(mkdtemp(dir));

	struct reftable_write_options cfg = { 0 };
	struct reftable_stack *st = NULL;
	int err = reftable_new_stack(&st, dir, cfg);
	assert_err(err);

	struct reftable_ref_record refs[2] = { 0 };
	struct reftable_log_record logs[2] = { 0 };
	int N = ARRAY_SIZE(refs);
	for (i = 0; i < N; i++) {
		char buf[256];
		snprintf(buf, sizeof(buf), "branch%02d", i);
		refs[i].ref_name = xstrdup(buf);
		refs[i].value = reftable_malloc(SHA1_SIZE);
		refs[i].update_index = i + 1;
		set_test_hash(refs[i].value, i);

		logs[i].ref_name = xstrdup(buf);
		logs[i].update_index = N + i + 1;
		logs[i].new_hash = reftable_malloc(SHA1_SIZE);
		logs[i].email = xstrdup("identity@invalid");
		set_test_hash(logs[i].new_hash, i);
	}

	for (i = 0; i < N; i++) {
		int err = reftable_stack_add(st, &write_test_ref, &refs[i]);
		assert_err(err);
	}

	for (i = 0; i < N; i++) {
		int err = reftable_stack_add(st, &write_test_log, &logs[i]);
		assert_err(err);
	}

	err = reftable_stack_compact_all(st, NULL);
	assert_err(err);

	for (i = 0; i < N; i++) {
		struct reftable_ref_record dest = { 0 };
		int err = reftable_stack_read_ref(st, refs[i].ref_name, &dest);
		assert_err(err);
		assert(reftable_ref_record_equal(&dest, refs + i, SHA1_SIZE));
		reftable_ref_record_clear(&dest);
	}

	for (i = 0; i < N; i++) {
		struct reftable_log_record dest = { 0 };
		int err = reftable_stack_read_log(st, refs[i].ref_name, &dest);
		assert_err(err);
		assert(reftable_log_record_equal(&dest, logs + i, SHA1_SIZE));
		reftable_log_record_clear(&dest);
	}

	struct reftable_write_options cfg32 = { .hash_id = SHA256_ID };
	struct reftable_stack *st32 = NULL;
	err = reftable_new_stack(&st32, dir, cfg32);
	assert(err == FORMAT_ERROR);

	/* cleanup */
	reftable_stack_destroy(st);
	for (i = 0; i < N; i++) {
		reftable_ref_record_clear(&refs[i]);
		reftable_log_record_clear(&logs[i]);
	}
}

void test_log2(void)
{
	assert(1 == fastlog2(3));
	assert(2 == fastlog2(4));
	assert(2 == fastlog2(5));
}

void test_sizes_to_segments(void)
{
	uint64_t sizes[] = { 2, 3, 4, 5, 7, 9 };
	/* .................0  1  2  3  4  5 */

	int seglen = 0;
	struct segment *segs =
		sizes_to_segments(&seglen, sizes, ARRAY_SIZE(sizes));
	assert(segs[2].log == 3);
	assert(segs[2].start == 5);
	assert(segs[2].end == 6);

	assert(segs[1].log == 2);
	assert(segs[1].start == 2);
	assert(segs[1].end == 5);
	reftable_free(segs);
}

void test_sizes_to_segments_empty(void)
{
	uint64_t sizes[] = {};
	/* .................0  1  2  3  4  5 */

	int seglen = 0;
	struct segment *segs =
		sizes_to_segments(&seglen, sizes, ARRAY_SIZE(sizes));
	assert(seglen == 0);
	reftable_free(segs);
}

void test_suggest_compaction_segment(void)
{
	{
		uint64_t sizes[] = { 128, 64, 17, 16, 9, 9, 9, 16, 16 };
		/* .................0    1    2  3   4  5  6 */
		struct segment min =
			suggest_compaction_segment(sizes, ARRAY_SIZE(sizes));
		assert(min.start == 2);
		assert(min.end == 7);
	}

	{
		uint64_t sizes[] = { 64, 32, 16, 8, 4, 2 };
		struct segment result =
			suggest_compaction_segment(sizes, ARRAY_SIZE(sizes));
		assert(result.start == result.end);
	}
}

void test_reflog_expire(void)
{
	char dir[256] = "/tmp/stack.test_reflog_expire.XXXXXX";
	assert(mkdtemp(dir));

	struct reftable_write_options cfg = { 0 };
	struct reftable_stack *st = NULL;
	int err = reftable_new_stack(&st, dir, cfg);
	assert_err(err);

	struct reftable_log_record logs[20] = { 0 };
	int N = ARRAY_SIZE(logs) - 1;
	int i = 0;
	for (i = 1; i <= N; i++) {
		char buf[256];
		snprintf(buf, sizeof(buf), "branch%02d", i);

		logs[i].ref_name = xstrdup(buf);
		logs[i].update_index = i;
		logs[i].time = i;
		logs[i].new_hash = reftable_malloc(SHA1_SIZE);
		logs[i].email = xstrdup("identity@invalid");
		set_test_hash(logs[i].new_hash, i);
	}

	for (i = 1; i <= N; i++) {
		int err = reftable_stack_add(st, &write_test_log, &logs[i]);
		assert_err(err);
	}

	err = reftable_stack_compact_all(st, NULL);
	assert_err(err);

	struct reftable_log_expiry_config expiry = {
		.time = 10,
	};
	err = reftable_stack_compact_all(st, &expiry);
	assert_err(err);

	struct reftable_log_record log = { 0 };
	err = reftable_stack_read_log(st, logs[9].ref_name, &log);
	assert(err == 1);

	err = reftable_stack_read_log(st, logs[11].ref_name, &log);
	assert_err(err);

	expiry.min_update_index = 15;
	err = reftable_stack_compact_all(st, &expiry);
	assert_err(err);

	err = reftable_stack_read_log(st, logs[14].ref_name, &log);
	assert(err == 1);

	err = reftable_stack_read_log(st, logs[16].ref_name, &log);
	assert_err(err);

	/* cleanup */
	reftable_stack_destroy(st);
	for (i = 0; i < N; i++) {
		reftable_log_record_clear(&logs[i]);
	}
}

int write_nothing(struct reftable_writer *wr, void *arg)
{
	reftable_writer_set_limits(wr, 1, 1);
	return 0;
}

void test_empty_add(void)
{
	char dir[256] = "/tmp/stack.test_reftable_stack_add.XXXXXX";
	assert(mkdtemp(dir));

	struct reftable_write_options cfg = { 0 };
	struct reftable_stack *st = NULL;
	int err = reftable_new_stack(&st, dir, cfg);
	assert_err(err);

	err = reftable_stack_add(st, &write_nothing, NULL);
	assert_err(err);

	struct reftable_stack *st2 = NULL;
	err = reftable_new_stack(&st2, dir, cfg);
	assert_err(err);
}

int main(int argc, char *argv[])
{
	add_test_case("test_reftable_stack_add_one",
		      &test_reftable_stack_add_one);
	add_test_case("test_empty_add", test_empty_add);
	add_test_case("test_reflog_expire", test_reflog_expire);
	add_test_case("test_suggest_compaction_segment",
		      &test_suggest_compaction_segment);
	add_test_case("test_sizes_to_segments", &test_sizes_to_segments);
	add_test_case("test_sizes_to_segments_empty",
		      &test_sizes_to_segments_empty);
	add_test_case("test_log2", &test_log2);
	add_test_case("test_parse_names", &test_parse_names);
	add_test_case("test_read_file", &test_read_file);
	add_test_case("test_names_equal", &test_names_equal);
	add_test_case("test_reftable_stack_add", &test_reftable_stack_add);
	test_main(argc, argv);
}
