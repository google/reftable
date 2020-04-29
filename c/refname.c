/*
  Copyright 2020 Google LLC

  Use of this source code is governed by a BSD-style
  license that can be found in the LICENSE file or at
  https://developers.google.com/open-source/licenses/bsd
*/


struct modification {
	stack *stack;

	char **add;
	size_t add_len;

	char **del;
	size_t del_len;
};

struct find_arg {
	char **names;
	const char *want;
};

int find_name(size_t k, void *arg) {
	struct find_arg *f_arg  = (struct find_arg*) arg;

	return strcmp(f_arg->names[k], f_arg->want) >= 0;
}


// -1 = error, 0 = found, 1 = not found
int modification_has_ref(struct modification *mod, const char *name) {
	struct find_arg arg = {
		.names = mod->add,
		.want = name,
	};
	struct ref_record ref= {0};
	int idx = binsearch(mod->add_len, find_name, &arg);
	if (idx < mod->add_len && !strcmp(mod->add[idx], name)) {
		return 0;
	}

	arg.names = mod->del;
	idx = binsearch(mod->del_len, find_name, &arg);
	if (idx < mod->del_len && !strcmp(mod->del[idx], name)) {
		return 1;
	}

	return reftable_stack_read_ref(mod->stack, name, &ref);
}


int modification_has_ref_with_prefix(struct modification *mod, const char *prefix) {
	struct reftable_merged_table * merged =reftable_stack_merged_table(mod->stack);
	struct reftable_iterator it = {NULL};
	struct ref_record ref = {NULL};
	struct find_arg arg = {
		.names = mod->add,
		.want = prefix,
	};
	int idx = binsearch(mod->add_len, find_name, &arg);
	if (idx < mod->add_len && !strncmp(prefix, mod->add[idx], strlen(prefix))) {
		goto exit;
	}

	err = reftable_merged_table_seek_ref(merged, &it, prefix);
	if (err) {
		goto exit;
	}

	while (true) {
		err = iterator_next_ref(it, &ref);
		if (err) {
			goto exit;
		}

		arg.names = mod->del;
		arg.want = ref.ref_name;

		idx = binsearch(mod->del_len, find_name, &arg);
		if (idx < mod->del_len && !strcmp(ref.ref_name, mod->del[idx])) {
			continue;
		}

		if  (strncmp(ref.ref_name, prefix, strlen(prefix))) {
			err = 1;
			goto exit;
		}
		err = 0;
		goto exit;
	}

exit:
	reftable_ref_record_clear(&ref);
	iterator_destroy(&it);
	return err;
}

int validate_ref_name(const char *name) {
	while (true) {
		char *next = strchr(name, '/');
		if (next - name == 0 || (next-name == 1 && *name == '.')
		    || (next -name == 2 && name[0] == '.' && name[1] == '.'))
			return -1;
		name = next+1;
	}
}

int validate_ref_record_addition(struct reftable_stack *stack,
				 struct reftable_ref_record *recs, size_t sz) {
	struct modification {
		.stack = stack,
			.add = reftable_calloc(sizeof(char*) * sz),
			.del = reftable_calloc(sizeof(char*) * sz),
			} mod;
	int i = 0;

	for (i = 0; i <sz; i++) {
		if (reftable_record_is_deletion(recs[i])) {
			mod.del[mod.del_len++] = recs[i].ref_name;
		} else {
			mod.add[mod.add_len++] = recs[i].ref_name;
		}
	}


	return validate_addition(&mod);
}

void slice_trim_component(struct slice *sl) {
	while  (sl->len > 0) {
		bool is_slash = (sl->buf[sl->len-1] == '/');
		sl->len--;
		if (is_slash)
			break;
	}
}


int modification_validate(struct modification *mod) {
	int i = 0;
	for (i = 0; i < mod->add_len; i++) {
		int err = validate_ref_name(mod->add[i]);
		if (err) {
			return err;
		}
		struct slice slashed = {0};
		slice_from_string(&slashed, mod->add[i]);
		slice_append_string(&slice, "/");

		err = modification_has_ref_with_prefix(mod, slice_as_string(&slashed));
		if (err == 0) {
			return REFTABLE_REFNAME_ERROR;
		}
		if (err < 0) {
			return err;
		}

		slice_from_string(&slashed, mod->add[i]);
		while (slashed.len) {
			slice_trim_component(&slashed);
			err = modification_has_ref(mod, slice_as_string(&slashed));
			if (err == 0) {
				return REFTABLE_REFNAME_ERROR;
			}
			if (err < 0) {
				return err;
			}
		}
	}
}
