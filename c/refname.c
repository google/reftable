/*
  Copyright 2020 Google LLC

  Use of this source code is governed by a BSD-style
  license that can be found in the LICENSE file or at
  https://developers.google.com/open-source/licenses/bsd
*/

#include "system.h"
#include "reftable.h"
#include "basics.h"
#include "refname.h"
#include "slice.h"

struct find_arg {
	char **names;
	const char *want;
};

static int find_name(size_t k, void *arg)
{
	struct find_arg *f_arg = (struct find_arg *)arg;
	return strcmp(f_arg->names[k], f_arg->want) >= 0;
}

int modification_has_ref(struct modification *mod, const char *name)
{
	struct reftable_ref_record ref = { 0 };
	int err = 0;

	if (mod->add_len > 0) {
		struct find_arg arg = {
			.names = mod->add,
			.want = name,
		};
		int idx = binsearch(mod->add_len, find_name, &arg);
		if (idx < mod->add_len && !strcmp(mod->add[idx], name)) {
			return 0;
		}
	}

	if (mod->del_len > 0) {
		struct find_arg arg = {
			.names = mod->del,
			.want = name,
		};
		int idx = binsearch(mod->del_len, find_name, &arg);
		if (idx < mod->del_len && !strcmp(mod->del[idx], name)) {
			return 1;
		}
	}

	err = reftable_table_read_ref(&mod->tab, name, &ref);
	reftable_ref_record_clear(&ref);
	return err;
}

static void modification_clear(struct modification *mod)
{
	/* don't delete the strings themselves; they're owned by ref records.
	 */
	FREE_AND_NULL(mod->add);
	FREE_AND_NULL(mod->del);
	mod->add_len = 0;
	mod->del_len = 0;
}

int modification_has_ref_with_prefix(struct modification *mod,
				     const char *prefix)
{
	struct reftable_iterator it = { NULL };
	struct reftable_ref_record ref = { NULL };
	int err = 0;

	if (mod->add_len > 0) {
		struct find_arg arg = {
			.names = mod->add,
			.want = prefix,
		};
		int idx = binsearch(mod->add_len, find_name, &arg);
		if (idx < mod->add_len &&
		    !strncmp(prefix, mod->add[idx], strlen(prefix))) {
			goto exit;
		}
	}
	err = reftable_table_seek_ref(&mod->tab, &it, prefix);
	if (err) {
		goto exit;
	}

	while (true) {
		err = reftable_iterator_next_ref(&it, &ref);
		if (err) {
			goto exit;
		}

		if (mod->del_len > 0) {
			struct find_arg arg = {
				.names = mod->del,
				.want = ref.ref_name,
			};
			int idx = binsearch(mod->del_len, find_name, &arg);
			if (idx < mod->del_len &&
			    !strcmp(ref.ref_name, mod->del[idx])) {
				continue;
			}
		}

		if (strncmp(ref.ref_name, prefix, strlen(prefix))) {
			err = 1;
			goto exit;
		}
		err = 0;
		goto exit;
	}

exit:
	reftable_ref_record_clear(&ref);
	reftable_iterator_destroy(&it);
	return err;
}

int validate_ref_name(const char *name)
{
	while (true) {
		char *next = strchr(name, '/');
		if (!*name) {
			return REFTABLE_REFNAME_ERROR;
		}
		if (!next) {
			return 0;
		}
		if (next - name == 0 || (next - name == 1 && *name == '.') ||
		    (next - name == 2 && name[0] == '.' && name[1] == '.'))
			return REFTABLE_REFNAME_ERROR;
		name = next + 1;
	}
	return 0;
}

int validate_ref_record_addition(struct reftable_table tab,
				 struct reftable_ref_record *recs, size_t sz)
{
	struct modification mod = {
		.tab = tab,
		.add = reftable_calloc(sizeof(char *) * sz),
		.del = reftable_calloc(sizeof(char *) * sz),
	};
	int i = 0;
	int err = 0;
	for (; i < sz; i++) {
		if (reftable_ref_record_is_deletion(&recs[i])) {
			mod.del[mod.del_len++] = recs[i].ref_name;
		} else {
			mod.add[mod.add_len++] = recs[i].ref_name;
		}
	}

	err = modification_validate(&mod);
	modification_clear(&mod);
	return err;
}

static void slice_trim_component(struct slice *sl)
{
	while (sl->len > 0) {
		bool is_slash = (sl->buf[sl->len - 1] == '/');
		sl->len--;
		if (is_slash)
			break;
	}
}

int modification_validate(struct modification *mod)
{
	struct slice slashed = { 0 };
	int err = 0;
	int i = 0;
	for (; i < mod->add_len; i++) {
		err = validate_ref_name(mod->add[i]);
		if (err) {
			goto exit;
		}
		slice_set_string(&slashed, mod->add[i]);
		slice_addstr(&slashed, "/");

		err = modification_has_ref_with_prefix(
			mod, slice_as_string(&slashed));
		if (err == 0) {
			err = REFTABLE_NAME_CONFLICT;
			goto exit;
		}
		if (err < 0) {
			goto exit;
		}

		slice_set_string(&slashed, mod->add[i]);
		while (slashed.len) {
			slice_trim_component(&slashed);
			err = modification_has_ref(mod,
						   slice_as_string(&slashed));
			if (err == 0) {
				err = REFTABLE_NAME_CONFLICT;
				goto exit;
			}
			if (err < 0) {
				goto exit;
			}
		}
	}
	err = 0;
exit:
	slice_release(&slashed);
	return err;
}
