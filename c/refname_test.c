/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "reftable.h"

#include "basics.h"
#include "block.h"
#include "constants.h"
#include "reader.h"
#include "record.h"
#include "refname.h"
#include "system.h"

#include "test_framework.h"

struct testcase {
	char *add;
	char *del;
	int error_code;
};

void test_conflict(void)
{
	struct reftable_write_options opts = { 0 };
	struct slice buf = { 0 };
	struct reftable_writer *w =
		reftable_new_writer(&slice_write_void, &buf, &opts);
	reftable_writer_set_limits(w, 1, 1);

	struct reftable_ref_record rec = {
		.ref_name = "a/b",
		.target = "destination", /* make sure it's not a symref. */
		.update_index = 1,
	};

	int err = reftable_writer_add_ref(w, &rec);
	assert_err(err);

	err = reftable_writer_close(w);
	assert_err(err);
	reftable_writer_free(w);

	struct reftable_block_source source = { 0 };
	block_source_from_slice(&source, &buf);

	struct reftable_reader *rd = NULL;
	err = reftable_new_reader(&rd, source, "filename");
	assert_err(err);

	struct reftable_table tab = { NULL };
	reftable_table_from_reader(&tab, rd);

	struct testcase cases[] = {
		{ "a/b/c", NULL, REFTABLE_NAME_CONFLICT },
		{ "b", NULL, 0 },
		{ "a", NULL, REFTABLE_NAME_CONFLICT },
		{ "a", "a/b", 0 },

		{ "p/", NULL, REFTABLE_REFNAME_ERROR },
		{ "p//q", NULL, REFTABLE_REFNAME_ERROR },
		{ "p/./q", NULL, REFTABLE_REFNAME_ERROR },
		{ "p/../q", NULL, REFTABLE_REFNAME_ERROR },

		{ "a/b/c", "a/b", 0 },
		{ NULL, "a//b", 0 },
	};
	for (int i = 0; i < ARRAY_SIZE(cases); i++) {
		struct modification mod = {
			.tab = tab,
		};

		if (cases[i].add != NULL) {
			mod.add = &cases[i].add;
			mod.add_len = 1;
		}
		if (cases[i].del != NULL) {
			mod.del = &cases[i].del;
			mod.del_len = 1;
		}

		int err = modification_validate(&mod);
		assert(err == cases[i].error_code);
	}

	reftable_reader_free(rd);
	slice_clear(&buf);
}

int main(int argc, char *argv[])
{
	add_test_case("test_conflict", &test_conflict);
	test_main(argc, argv);
}
