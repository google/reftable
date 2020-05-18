/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "reftable.h"
#include "record.h"
#include "reader.h"
#include "merged.h"

struct reftable_table_vtable {
	int (*seek)(void *tab, struct reftable_iterator *it,
		    struct reftable_record *);
};

static int reftable_reader_seek_void(void *tab, struct reftable_iterator *it,
				     struct reftable_record *rec)
{
	return reader_seek((struct reftable_reader *)tab, it, rec);
}

static struct reftable_table_vtable reader_vtable = {
	.seek = reftable_reader_seek_void,
};

static int reftable_merged_table_seek_void(void *tab,
					   struct reftable_iterator *it,
					   struct reftable_record *rec)
{
	return merged_table_seek_record((struct reftable_merged_table *)tab, it,
					rec);
}

static struct reftable_table_vtable merged_table_vtable = {
	.seek = reftable_merged_table_seek_void,
};

int reftable_table_seek_ref(struct reftable_table *tab,
			    struct reftable_iterator *it, const char *name)
{
	struct reftable_ref_record ref = {
		.ref_name = (char *)name,
	};
	struct reftable_record rec = { 0 };
	reftable_record_from_ref(&rec, &ref);
	return tab->ops->seek(tab->table_arg, it, &rec);
}

void reftable_table_from_reader(struct reftable_table *tab,
				struct reftable_reader *reader)
{
	assert(tab->ops == NULL);
	tab->ops = &reader_vtable;
	tab->table_arg = reader;
}

void reftable_table_from_merged_table(struct reftable_table *tab,
				      struct reftable_merged_table *merged)
{
	assert(tab->ops == NULL);
	tab->ops = &merged_table_vtable;
	tab->table_arg = merged;
}

int reftable_table_read_ref(struct reftable_table *tab, const char *name,
			    struct reftable_ref_record *ref)
{
	struct reftable_iterator it = { 0 };
	int err = reftable_table_seek_ref(tab, &it, name);
	if (err) {
		goto exit;
	}

	err = reftable_iterator_next_ref(&it, ref);
	if (err) {
		goto exit;
	}

	if (strcmp(ref->ref_name, name) ||
	    reftable_ref_record_is_deletion(ref)) {
		reftable_ref_record_clear(ref);
		err = 1;
		goto exit;
	}

exit:
	reftable_iterator_destroy(&it);
	return err;
}
