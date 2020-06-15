/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "reftable.h"
#include "reftable-tests.h"

static uint32_t hash_id;

static int dump_table(const char *tablename)
{
	struct reftable_block_source src = { 0 };
	int err = reftable_block_source_from_file(&src, tablename);
	struct reftable_iterator it = { 0 };
	struct reftable_ref_record ref = { 0 };
	struct reftable_log_record log = { 0 };
	struct reftable_reader *r = NULL;

	if (err < 0)
		return err;

	err = reftable_new_reader(&r, &src, tablename);
	if (err < 0)
		return err;

	err = reftable_reader_seek_ref(r, &it, "");
	if (err < 0) {
		return err;
	}

	while (1) {
		err = reftable_iterator_next_ref(&it, &ref);
		if (err > 0) {
			break;
		}
		if (err < 0) {
			return err;
		}
		reftable_ref_record_print(&ref, hash_id);
	}
	reftable_iterator_destroy(&it);
	reftable_ref_record_clear(&ref);

	err = reftable_reader_seek_log(r, &it, "");
	if (err < 0) {
		return err;
	}
	while (1) {
		err = reftable_iterator_next_log(&it, &log);
		if (err > 0) {
			break;
		}
		if (err < 0) {
			return err;
		}
		reftable_log_record_print(&log, hash_id);
	}
	reftable_iterator_destroy(&it);
	reftable_log_record_clear(&log);

	reftable_reader_free(r);
	return 0;
}

static int compact_stack(const char *stackdir)
{
	struct reftable_stack *stack = NULL;
	struct reftable_write_options cfg = {};

	int err = reftable_new_stack(&stack, stackdir, cfg);
	if (err < 0)
		goto done;

	err = reftable_stack_compact_all(stack, NULL);
	if (err < 0)
		goto done;
done:
	if (stack != NULL) {
		reftable_stack_destroy(stack);
	}
	return err;
}

static int dump_stack(const char *stackdir)
{
	struct reftable_stack *stack = NULL;
	struct reftable_write_options cfg = {};
	struct reftable_iterator it = { 0 };
	struct reftable_ref_record ref = { 0 };
	struct reftable_log_record log = { 0 };
	struct reftable_merged_table *merged = NULL;

	int err = reftable_new_stack(&stack, stackdir, cfg);
	if (err < 0)
		return err;

	merged = reftable_stack_merged_table(stack);

	err = reftable_merged_table_seek_ref(merged, &it, "");
	if (err < 0) {
		return err;
	}

	while (1) {
		err = reftable_iterator_next_ref(&it, &ref);
		if (err > 0) {
			break;
		}
		if (err < 0) {
			return err;
		}
		reftable_ref_record_print(&ref, hash_id);
	}
	reftable_iterator_destroy(&it);
	reftable_ref_record_clear(&ref);

	err = reftable_merged_table_seek_log(merged, &it, "");
	if (err < 0) {
		return err;
	}
	while (1) {
		err = reftable_iterator_next_log(&it, &log);
		if (err > 0) {
			break;
		}
		if (err < 0) {
			return err;
		}
		reftable_log_record_print(&log, hash_id);
	}
	reftable_iterator_destroy(&it);
	reftable_log_record_clear(&log);

	reftable_stack_destroy(stack);
	return 0;
}

static void print_help(void)
{
	printf("usage: dump [-cst] arg\n\n"
	       "options: \n"
	       "  -c compact\n"
	       "  -t dump table\n"
	       "  -s dump stack\n"
	       "  -h this help\n"
	       "  -2 use SHA256\n"
	       "\n");
}

int reftable_dump_main(int argc, char *const *argv)
{
	int err = 0;
	int opt;
	int opt_dump_table = 0;
	int opt_dump_stack = 0;
	int opt_compact = 0;
	const char *arg = NULL;
	while ((opt = getopt(argc, argv, "2chts")) != -1) {
		switch (opt) {
		case '2':
			hash_id = 0x73323536;
			break;
		case 't':
			opt_dump_table = 1;
			break;
		case 's':
			opt_dump_stack = 1;
			break;
		case 'c':
			opt_compact = 1;
			break;
		case '?':
		case 'h':
			print_help();
			return 2;
			break;
		}
	}

	if (argv[optind] == NULL) {
		fprintf(stderr, "need argument\n");
		print_help();
		return 2;
	}

	arg = argv[optind];

	if (opt_dump_table) {
		err = dump_table(arg);
	} else if (opt_dump_stack) {
		err = dump_stack(arg);
	} else if (opt_compact) {
		err = compact_stack(arg);
	}

	if (err < 0) {
		fprintf(stderr, "%s: %s: %s\n", argv[0], arg,
			reftable_error_str(err));
		return 1;
	}
	return 0;
}
