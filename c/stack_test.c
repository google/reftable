/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "stack.h"

#include "system.h"

#include "merged.h"
#include "basics.h"
#include "constants.h"
#include "record.h"
#include "test_framework.h"
#include "reftable-tests.h"

#include <sys/types.h>
#include <dirent.h>

static void clear_dir(const char *dirname)
{
	struct strbuf path = STRBUF_INIT;
	strbuf_addstr(&path, dirname);
	remove_dir_recursively(&path, 0);
	strbuf_release(&path);
}

static void test_read_file(void)
{
	char fn[256] = "/tmp/stack.test_read_file.XXXXXX";
	int fd = mkstemp(fn);
	char out[1024] = "line1\n\nline2\nline3";
	int n, err;
	char **names = NULL;
	char *want[] = { "line1", "line2", "line3" };
	int i = 0;

	EXPECT(fd > 0);
	n = write(fd, out, strlen(out));
	EXPECT(n == strlen(out));
	err = close(fd);
	EXPECT(err >= 0);

	err = read_lines(fn, &names);
	EXPECT_ERR(err);

	for (i = 0; names[i] != NULL; i++) {
		EXPECT(0 == strcmp(want[i], names[i]));
	}
	free_names(names);
	remove(fn);
}

static void test_parse_names(void)
{
	char buf[] = "line\n";
	char **names = NULL;
	parse_names(buf, strlen(buf), &names);

	EXPECT(NULL != names[0]);
	EXPECT(0 == strcmp(names[0], "line"));
	EXPECT(NULL == names[1]);
	free_names(names);
}

static void test_names_equal(void)
{
	char *a[] = { "a", "b", "c", NULL };
	char *b[] = { "a", "b", "d", NULL };
	char *c[] = { "a", "b", NULL };

	EXPECT(names_equal(a, a));
	EXPECT(!names_equal(a, b));
	EXPECT(!names_equal(a, c));
}

static int write_test_ref(struct reftable_writer *wr, void *arg)
{
	struct reftable_ref_record *ref = arg;
	reftable_writer_set_limits(wr, ref->update_index, ref->update_index);
	return reftable_writer_add_ref(wr, ref);
}

struct write_log_arg {
	struct reftable_log_record *log;
	uint64_t update_index;
};

static int write_test_log(struct reftable_writer *wr, void *arg)
{
	struct write_log_arg *wla = arg;

	reftable_writer_set_limits(wr, wla->update_index, wla->update_index);
	return reftable_writer_add_log(wr, wla->log);
}

static void test_reftable_stack_add_one(void)
{
	char dir[256] = "/tmp/stack_test.XXXXXX";
	struct reftable_write_options cfg = { 0 };
	struct reftable_stack *st = NULL;
	int err;
	struct reftable_ref_record ref = {
		.refname = "HEAD",
		.update_index = 1,
		.value_type = REFTABLE_REF_SYMREF,
		.value.symref = "master",
	};
	struct reftable_ref_record dest = { NULL };

	EXPECT(mkdtemp(dir));

	err = reftable_new_stack(&st, dir, cfg);
	EXPECT_ERR(err);

	err = reftable_stack_add(st, &write_test_ref, &ref);
	EXPECT_ERR(err);

	err = reftable_stack_read_ref(st, ref.refname, &dest);
	EXPECT_ERR(err);
	EXPECT(0 == strcmp("master", dest.value.symref));

	reftable_ref_record_release(&dest);
	reftable_stack_destroy(st);
	clear_dir(dir);
}

static void test_reftable_stack_uptodate(void)
{
	struct reftable_write_options cfg = { 0 };
	struct reftable_stack *st1 = NULL;
	struct reftable_stack *st2 = NULL;
	char dir[256] = "/tmp/stack_test.XXXXXX";
	int err;
	struct reftable_ref_record ref1 = {
		.refname = "HEAD",
		.update_index = 1,
		.value_type = REFTABLE_REF_SYMREF,
		.value.symref = "master",
	};
	struct reftable_ref_record ref2 = {
		.refname = "branch2",
		.update_index = 2,
		.value_type = REFTABLE_REF_SYMREF,
		.value.symref = "master",
	};

	EXPECT(mkdtemp(dir));

	err = reftable_new_stack(&st1, dir, cfg);
	EXPECT_ERR(err);

	err = reftable_new_stack(&st2, dir, cfg);
	EXPECT_ERR(err);

	err = reftable_stack_add(st1, &write_test_ref, &ref1);
	EXPECT_ERR(err);

	err = reftable_stack_add(st2, &write_test_ref, &ref2);
	EXPECT(err == REFTABLE_LOCK_ERROR);

	err = reftable_stack_reload(st2);
	EXPECT_ERR(err);

	err = reftable_stack_add(st2, &write_test_ref, &ref2);
	EXPECT_ERR(err);
	reftable_stack_destroy(st1);
	reftable_stack_destroy(st2);
	clear_dir(dir);
}

static void test_reftable_stack_transaction_api(void)
{
	char dir[256] = "/tmp/stack_test.XXXXXX";
	struct reftable_write_options cfg = { 0 };
	struct reftable_stack *st = NULL;
	int err;
	struct reftable_addition *add = NULL;

	struct reftable_ref_record ref = {
		.refname = "HEAD",
		.update_index = 1,
		.value_type = REFTABLE_REF_SYMREF,
		.value.symref = "master",
	};
	struct reftable_ref_record dest = { NULL };

	EXPECT(mkdtemp(dir));

	err = reftable_new_stack(&st, dir, cfg);
	EXPECT_ERR(err);

	reftable_addition_destroy(add);

	err = reftable_stack_new_addition(&add, st);
	EXPECT_ERR(err);

	err = reftable_addition_add(add, &write_test_ref, &ref);
	EXPECT_ERR(err);

	err = reftable_addition_commit(add);
	EXPECT_ERR(err);

	reftable_addition_destroy(add);

	err = reftable_stack_read_ref(st, ref.refname, &dest);
	EXPECT_ERR(err);
	EXPECT(REFTABLE_REF_SYMREF == dest.value_type);
	EXPECT(0 == strcmp("master", dest.value.symref));

	reftable_ref_record_release(&dest);
	reftable_stack_destroy(st);
	clear_dir(dir);
}

static void test_reftable_stack_validate_refname(void)
{
	struct reftable_write_options cfg = { 0 };
	struct reftable_stack *st = NULL;
	int err;
	char dir[256] = "/tmp/stack_test.XXXXXX";
	int i;
	struct reftable_ref_record ref = {
		.refname = "a/b",
		.update_index = 1,
		.value_type = REFTABLE_REF_SYMREF,
		.value.symref = "master",
	};
	char *additions[] = { "a", "a/b/c" };

	EXPECT(mkdtemp(dir));
	err = reftable_new_stack(&st, dir, cfg);
	EXPECT_ERR(err);

	err = reftable_stack_add(st, &write_test_ref, &ref);
	EXPECT_ERR(err);

	for (i = 0; i < ARRAY_SIZE(additions); i++) {
		struct reftable_ref_record ref = {
			.refname = additions[i],
			.update_index = 1,
			.value_type = REFTABLE_REF_SYMREF,
			.value.symref = "master",
		};

		err = reftable_stack_add(st, &write_test_ref, &ref);
		EXPECT(err == REFTABLE_NAME_CONFLICT);
	}

	reftable_stack_destroy(st);
	clear_dir(dir);
}

static int write_error(struct reftable_writer *wr, void *arg)
{
	return *((int *)arg);
}

static void test_reftable_stack_update_index_check(void)
{
	char dir[256] = "/tmp/stack_test.XXXXXX";
	struct reftable_write_options cfg = { 0 };
	struct reftable_stack *st = NULL;
	int err;
	struct reftable_ref_record ref1 = {
		.refname = "name1",
		.update_index = 1,
		.value_type = REFTABLE_REF_SYMREF,
		.value.symref = "master",
	};
	struct reftable_ref_record ref2 = {
		.refname = "name2",
		.update_index = 1,
		.value_type = REFTABLE_REF_SYMREF,
		.value.symref = "master",
	};
	EXPECT(mkdtemp(dir));

	err = reftable_new_stack(&st, dir, cfg);
	EXPECT_ERR(err);

	err = reftable_stack_add(st, &write_test_ref, &ref1);
	EXPECT_ERR(err);

	err = reftable_stack_add(st, &write_test_ref, &ref2);
	EXPECT(err == REFTABLE_API_ERROR);
	reftable_stack_destroy(st);
	clear_dir(dir);
}

static void test_reftable_stack_lock_failure(void)
{
	char dir[256] = "/tmp/stack_test.XXXXXX";
	struct reftable_write_options cfg = { 0 };
	struct reftable_stack *st = NULL;
	int err, i;
	EXPECT(mkdtemp(dir));

	err = reftable_new_stack(&st, dir, cfg);
	EXPECT_ERR(err);
	for (i = -1; i != REFTABLE_EMPTY_TABLE_ERROR; i--) {
		err = reftable_stack_add(st, &write_error, &i);
		EXPECT(err == i);
	}

	reftable_stack_destroy(st);
	clear_dir(dir);
}

static void test_reftable_stack_add(void)
{
	int i = 0;
	int err = 0;
	struct reftable_write_options cfg = {
		.exact_log_message = 1,
	};
	struct reftable_stack *st = NULL;
	char dir[256] = "/tmp/stack_test.XXXXXX";
	struct reftable_ref_record refs[2] = { { NULL } };
	struct reftable_log_record logs[2] = { { NULL } };
	int N = ARRAY_SIZE(refs);

	EXPECT(mkdtemp(dir));

	err = reftable_new_stack(&st, dir, cfg);
	EXPECT_ERR(err);
	st->disable_auto_compact = 1;

	for (i = 0; i < N; i++) {
		char buf[256];
		snprintf(buf, sizeof(buf), "branch%02d", i);
		refs[i].refname = xstrdup(buf);
		refs[i].update_index = i + 1;
		refs[i].value_type = REFTABLE_REF_VAL1;
		refs[i].value.val1 = reftable_malloc(SHA1_SIZE);
		set_test_hash(refs[i].value.val1, i);

		logs[i].refname = xstrdup(buf);
		logs[i].update_index = N + i + 1;
		logs[i].new_hash = reftable_malloc(SHA1_SIZE);
		logs[i].email = xstrdup("identity@invalid");
		set_test_hash(logs[i].new_hash, i);
	}

	for (i = 0; i < N; i++) {
		int err = reftable_stack_add(st, &write_test_ref, &refs[i]);
		EXPECT_ERR(err);
	}

	for (i = 0; i < N; i++) {
		struct write_log_arg arg = {
			.log = &logs[i],
			.update_index = reftable_stack_next_update_index(st),
		};
		int err = reftable_stack_add(st, &write_test_log, &arg);
		EXPECT_ERR(err);
	}

	err = reftable_stack_compact_all(st, NULL);
	EXPECT_ERR(err);

	for (i = 0; i < N; i++) {
		struct reftable_ref_record dest = { NULL };

		int err = reftable_stack_read_ref(st, refs[i].refname, &dest);
		EXPECT_ERR(err);
		EXPECT(reftable_ref_record_equal(&dest, refs + i, SHA1_SIZE));
		reftable_ref_record_release(&dest);
	}

	for (i = 0; i < N; i++) {
		struct reftable_log_record dest = { NULL };
		int err = reftable_stack_read_log(st, refs[i].refname, &dest);
		EXPECT_ERR(err);
		EXPECT(reftable_log_record_equal(&dest, logs + i, SHA1_SIZE));
		reftable_log_record_release(&dest);
	}

	/* cleanup */
	reftable_stack_destroy(st);
	for (i = 0; i < N; i++) {
		reftable_ref_record_release(&refs[i]);
		reftable_log_record_release(&logs[i]);
	}
	clear_dir(dir);
}

static void test_reftable_stack_log_normalize(void)
{
	int err = 0;
	struct reftable_write_options cfg = {
		0,
	};
	struct reftable_stack *st = NULL;
	char dir[256] = "/tmp/stack_test.XXXXXX";

	uint8_t h1[SHA1_SIZE] = { 0x01 }, h2[SHA1_SIZE] = { 0x02 };

	struct reftable_log_record input = {
		.refname = "branch",
		.update_index = 1,
		.new_hash = h1,
		.old_hash = h2,
	};
	struct reftable_log_record dest = {
		.update_index = 0,
	};
	struct write_log_arg arg = {
		.log = &input,
		.update_index = 1,
	};

	EXPECT(mkdtemp(dir));
	err = reftable_new_stack(&st, dir, cfg);
	EXPECT_ERR(err);

	input.message = "one\ntwo";
	err = reftable_stack_add(st, &write_test_log, &arg);
	EXPECT(err == REFTABLE_API_ERROR);

	input.message = "one";
	err = reftable_stack_add(st, &write_test_log, &arg);
	EXPECT_ERR(err);

	err = reftable_stack_read_log(st, input.refname, &dest);
	EXPECT_ERR(err);
	EXPECT(0 == strcmp(dest.message, "one\n"));

	input.message = "two\n";
	arg.update_index = 2;
	err = reftable_stack_add(st, &write_test_log, &arg);
	EXPECT_ERR(err);
	err = reftable_stack_read_log(st, input.refname, &dest);
	EXPECT_ERR(err);
	EXPECT(0 == strcmp(dest.message, "two\n"));

	/* cleanup */
	reftable_stack_destroy(st);
	reftable_log_record_release(&dest);
	clear_dir(dir);
}

static void test_reftable_stack_tombstone(void)
{
	int i = 0;
	char dir[256] = "/tmp/stack_test.XXXXXX";
	struct reftable_write_options cfg = { 0 };
	struct reftable_stack *st = NULL;
	int err;
	struct reftable_ref_record refs[2] = { { NULL } };
	struct reftable_log_record logs[2] = { { NULL } };
	int N = ARRAY_SIZE(refs);
	struct reftable_ref_record dest = { NULL };
	struct reftable_log_record log_dest = { NULL };

	EXPECT(mkdtemp(dir));

	err = reftable_new_stack(&st, dir, cfg);
	EXPECT_ERR(err);

	for (i = 0; i < N; i++) {
		const char *buf = "branch";
		refs[i].refname = xstrdup(buf);
		refs[i].update_index = i + 1;
		if (i % 2 == 0) {
			refs[i].value_type = REFTABLE_REF_VAL1;
			refs[i].value.val1 = reftable_malloc(SHA1_SIZE);
			set_test_hash(refs[i].value.val1, i);
		}
		logs[i].refname = xstrdup(buf);
		/* update_index is part of the key. */
		logs[i].update_index = 42;
		if (i % 2 == 0) {
			logs[i].new_hash = reftable_malloc(SHA1_SIZE);
			set_test_hash(logs[i].new_hash, i);
			logs[i].email = xstrdup("identity@invalid");
		}
	}
	for (i = 0; i < N; i++) {
		int err = reftable_stack_add(st, &write_test_ref, &refs[i]);
		EXPECT_ERR(err);
	}
	for (i = 0; i < N; i++) {
		struct write_log_arg arg = {
			.log = &logs[i],
			.update_index = reftable_stack_next_update_index(st),
		};
		int err = reftable_stack_add(st, &write_test_log, &arg);
		EXPECT_ERR(err);
	}

	err = reftable_stack_read_ref(st, "branch", &dest);
	EXPECT(err == 1);
	reftable_ref_record_release(&dest);

	err = reftable_stack_read_log(st, "branch", &log_dest);
	EXPECT(err == 1);
	reftable_log_record_release(&log_dest);

	err = reftable_stack_compact_all(st, NULL);
	EXPECT_ERR(err);

	err = reftable_stack_read_ref(st, "branch", &dest);
	EXPECT(err == 1);

	err = reftable_stack_read_log(st, "branch", &log_dest);
	EXPECT(err == 1);
	reftable_ref_record_release(&dest);
	reftable_log_record_release(&log_dest);

	/* cleanup */
	reftable_stack_destroy(st);
	for (i = 0; i < N; i++) {
		reftable_ref_record_release(&refs[i]);
		reftable_log_record_release(&logs[i]);
	}
	clear_dir(dir);
}

static void test_reftable_stack_hash_id(void)
{
	char dir[256] = "/tmp/stack_test.XXXXXX";
	struct reftable_write_options cfg = { 0 };
	struct reftable_stack *st = NULL;
	int err;

	struct reftable_ref_record ref = {
		.refname = "master",
		.value_type = REFTABLE_REF_SYMREF,
		.value.symref = "target",
		.update_index = 1,
	};
	struct reftable_write_options cfg32 = { .hash_id = SHA256_ID };
	struct reftable_stack *st32 = NULL;
	struct reftable_write_options cfg_default = { 0 };
	struct reftable_stack *st_default = NULL;
	struct reftable_ref_record dest = { NULL };

	EXPECT(mkdtemp(dir));
	err = reftable_new_stack(&st, dir, cfg);
	EXPECT_ERR(err);

	err = reftable_stack_add(st, &write_test_ref, &ref);
	EXPECT_ERR(err);

	/* can't read it with the wrong hash ID. */
	err = reftable_new_stack(&st32, dir, cfg32);
	EXPECT(err == REFTABLE_FORMAT_ERROR);

	/* check that we can read it back with default config too. */
	err = reftable_new_stack(&st_default, dir, cfg_default);
	EXPECT_ERR(err);

	err = reftable_stack_read_ref(st_default, "master", &dest);
	EXPECT_ERR(err);

	EXPECT(reftable_ref_record_equal(&ref, &dest, SHA1_SIZE));
	reftable_ref_record_release(&dest);
	reftable_stack_destroy(st);
	reftable_stack_destroy(st_default);
	clear_dir(dir);
}

static void test_log2(void)
{
	EXPECT(1 == fastlog2(3));
	EXPECT(2 == fastlog2(4));
	EXPECT(2 == fastlog2(5));
}

static void test_sizes_to_segments(void)
{
	uint64_t sizes[] = { 2, 3, 4, 5, 7, 9 };
	/* .................0  1  2  3  4  5 */

	int seglen = 0;
	struct segment *segs =
		sizes_to_segments(&seglen, sizes, ARRAY_SIZE(sizes));
	EXPECT(segs[2].log == 3);
	EXPECT(segs[2].start == 5);
	EXPECT(segs[2].end == 6);

	EXPECT(segs[1].log == 2);
	EXPECT(segs[1].start == 2);
	EXPECT(segs[1].end == 5);
	reftable_free(segs);
}

static void test_sizes_to_segments_empty(void)
{
	uint64_t sizes[0];

	int seglen = 0;
	struct segment *segs =
		sizes_to_segments(&seglen, sizes, ARRAY_SIZE(sizes));
	EXPECT(seglen == 0);
	reftable_free(segs);
}

static void test_sizes_to_segments_all_equal(void)
{
	uint64_t sizes[] = { 5, 5 };

	int seglen = 0;
	struct segment *segs =
		sizes_to_segments(&seglen, sizes, ARRAY_SIZE(sizes));
	EXPECT(seglen == 1);
	EXPECT(segs[0].start == 0);
	EXPECT(segs[0].end == 2);
	reftable_free(segs);
}

static void test_suggest_compaction_segment(void)
{
	uint64_t sizes[] = { 128, 64, 17, 16, 9, 9, 9, 16, 16 };
	/* .................0    1    2  3   4  5  6 */
	struct segment min =
		suggest_compaction_segment(sizes, ARRAY_SIZE(sizes));
	EXPECT(min.start == 2);
	EXPECT(min.end == 7);
}

static void test_suggest_compaction_segment_nothing(void)
{
	uint64_t sizes[] = { 64, 32, 16, 8, 4, 2 };
	struct segment result =
		suggest_compaction_segment(sizes, ARRAY_SIZE(sizes));
	EXPECT(result.start == result.end);
}

static void test_reflog_expire(void)
{
	char dir[256] = "/tmp/stack.test_reflog_expire.XXXXXX";
	struct reftable_write_options cfg = { 0 };
	struct reftable_stack *st = NULL;
	struct reftable_log_record logs[20] = { { NULL } };
	int N = ARRAY_SIZE(logs) - 1;
	int i = 0;
	int err;
	struct reftable_log_expiry_config expiry = {
		.time = 10,
	};
	struct reftable_log_record log = { NULL };

	EXPECT(mkdtemp(dir));

	err = reftable_new_stack(&st, dir, cfg);
	EXPECT_ERR(err);

	for (i = 1; i <= N; i++) {
		char buf[256];
		snprintf(buf, sizeof(buf), "branch%02d", i);

		logs[i].refname = xstrdup(buf);
		logs[i].update_index = i;
		logs[i].time = i;
		logs[i].new_hash = reftable_malloc(SHA1_SIZE);
		logs[i].email = xstrdup("identity@invalid");
		set_test_hash(logs[i].new_hash, i);
	}

	for (i = 1; i <= N; i++) {
		struct write_log_arg arg = {
			.log = &logs[i],
			.update_index = reftable_stack_next_update_index(st),
		};
		int err = reftable_stack_add(st, &write_test_log, &arg);
		EXPECT_ERR(err);
	}

	err = reftable_stack_compact_all(st, NULL);
	EXPECT_ERR(err);

	err = reftable_stack_compact_all(st, &expiry);
	EXPECT_ERR(err);

	err = reftable_stack_read_log(st, logs[9].refname, &log);
	EXPECT(err == 1);

	err = reftable_stack_read_log(st, logs[11].refname, &log);
	EXPECT_ERR(err);

	expiry.min_update_index = 15;
	err = reftable_stack_compact_all(st, &expiry);
	EXPECT_ERR(err);

	err = reftable_stack_read_log(st, logs[14].refname, &log);
	EXPECT(err == 1);

	err = reftable_stack_read_log(st, logs[16].refname, &log);
	EXPECT_ERR(err);

	/* cleanup */
	reftable_stack_destroy(st);
	for (i = 0; i <= N; i++) {
		reftable_log_record_release(&logs[i]);
	}
	clear_dir(dir);
	reftable_log_record_release(&log);
}

static int write_nothing(struct reftable_writer *wr, void *arg)
{
	reftable_writer_set_limits(wr, 1, 1);
	return 0;
}

static void test_empty_add(void)
{
	struct reftable_write_options cfg = { 0 };
	struct reftable_stack *st = NULL;
	int err;
	char dir[256] = "/tmp/stack_test.XXXXXX";
	struct reftable_stack *st2 = NULL;

	EXPECT(mkdtemp(dir));

	err = reftable_new_stack(&st, dir, cfg);
	EXPECT_ERR(err);

	err = reftable_stack_add(st, &write_nothing, NULL);
	EXPECT_ERR(err);

	err = reftable_new_stack(&st2, dir, cfg);
	EXPECT_ERR(err);
	clear_dir(dir);
	reftable_stack_destroy(st);
	reftable_stack_destroy(st2);
}

static void test_reftable_stack_auto_compaction(void)
{
	struct reftable_write_options cfg = { 0 };
	struct reftable_stack *st = NULL;
	char dir[256] = "/tmp/stack_test.XXXXXX";
	int err, i;
	int N = 100;
	EXPECT(mkdtemp(dir));

	err = reftable_new_stack(&st, dir, cfg);
	EXPECT_ERR(err);

	for (i = 0; i < N; i++) {
		char name[100];
		struct reftable_ref_record ref = {
			.refname = name,
			.update_index = reftable_stack_next_update_index(st),
			.value_type = REFTABLE_REF_SYMREF,
			.value.symref = "master",
		};
		snprintf(name, sizeof(name), "branch%04d", i);

		err = reftable_stack_add(st, &write_test_ref, &ref);
		EXPECT_ERR(err);

		EXPECT(i < 3 || st->merged->stack_len < 2 * fastlog2(i));
	}

	EXPECT(reftable_stack_compaction_stats(st)->entries_written <
	       (uint64_t)(N * fastlog2(N)));

	reftable_stack_destroy(st);
	clear_dir(dir);
}

int stack_test_main(int argc, const char *argv[])
{
	test_reftable_stack_uptodate();
	test_reftable_stack_transaction_api();
	test_reftable_stack_hash_id();
	test_sizes_to_segments_all_equal();
	test_reftable_stack_auto_compaction();
	test_reftable_stack_validate_refname();
	test_reftable_stack_update_index_check();
	test_reftable_stack_lock_failure();
	test_reftable_stack_log_normalize();
	test_reftable_stack_tombstone();
	test_reftable_stack_add_one();
	test_empty_add();
	test_reflog_expire();
	test_suggest_compaction_segment();
	test_suggest_compaction_segment_nothing();
	test_sizes_to_segments();
	test_sizes_to_segments_empty();
	test_log2();
	test_parse_names();
	test_read_file();
	test_names_equal();
	test_reftable_stack_add();
	return 0;
}
