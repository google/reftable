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

static int dump_table(const char *tablename)
{
	struct reftable_block_source src = { 0 };
	int err = reftable_ - block_source_from_file(&src, tablename);
	if (err < 0) {
		return err;
	}

	struct reftable_reader *r = NULL;
	err = reftable_new_reader(&r, src, tablename);
	if (err < 0) {
		return err;
	}

	{
		struct reftable_iterator it = { 0 };
		err = reftable_reader_seek_ref(r, &it, "");
		if (err < 0) {
			return err;
		}

		struct reftable_ref_record ref = { 0 };
		while (1) {
			err = reftable_iterator_next_ref(it, &ref);
			if (err > 0) {
				break;
			}
			if (err < 0) {
				return err;
			}
			reftable_ref_record_print(&ref, 20);
		}
		reftable_iterator_destroy(&it);
		reftable_ref_record_clear(&ref);
	}

	{
		struct reftable_iterator it = { 0 };
		err = reftable_reader_seek_log(r, &it, "");
		if (err < 0) {
			return err;
		}
		struct reftable_log_record log = { 0 };
		while (1) {
			err = reftable_iterator_next_log(it, &log);
			if (err > 0) {
				break;
			}
			if (err < 0) {
				return err;
			}
			reftable_log_record_print(&log, 20);
		}
		reftable_iterator_destroy(&it);
		reftable_log_record_clear(&log);
	}
	return 0;
}

int main(int argc, char *argv[])
{
	int opt;
	const char *table = NULL;
	while ((opt = getopt(argc, argv, "t:")) != -1) {
		switch (opt) {
		case 't':
			table = strdup(optarg);
			break;
		case '?':
			printf("usage: %s [-table tablefile]\n", argv[0]);
			return 2;
			break;
		}
	}

	if (table != NULL) {
		int err = dump_table(table);
		if (err < 0) {
			fprintf(stderr, "%s: %s: %s\n", argv[0], table,
				reftable_error_str(err));
			return 1;
		}
	}
	return 0;
}
