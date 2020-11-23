/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "merged.h"

#include "constants.h"
#include "iter.h"
#include "pq.h"
#include "reader.h"
#include "record.h"
#include "reftable-merged.h"
#include "reftable-error.h"
#include "system.h"

static int merged_iter_init(struct merged_iter *mi)
{
	int i = 0;
	for (i = 0; i < mi->stack_len; i++) {
		struct reftable_record rec = reftable_new_record(mi->typ);
		int err = iterator_next(&mi->stack[i], &rec);
		if (err < 0) {
			return err;
		}

		if (err > 0) {
			reftable_iterator_destroy(&mi->stack[i]);
			reftable_record_destroy(&rec);
		} else {
			struct pq_entry e = {
				.rec = rec,
				.index = i,
			};
			merged_iter_pqueue_add(&mi->pq, e);
		}
	}

	return 0;
}

static void merged_iter_close(void *p)
{
	struct merged_iter *mi = (struct merged_iter *)p;
	int i = 0;
	merged_iter_pqueue_release(&mi->pq);
	for (i = 0; i < mi->stack_len; i++) {
		reftable_iterator_destroy(&mi->stack[i]);
	}
	reftable_free(mi->stack);
}

static int merged_iter_advance_nonnull_subiter(struct merged_iter *mi,
					       size_t idx)
{
	struct reftable_record rec = reftable_new_record(mi->typ);
	struct pq_entry e = {
		.rec = rec,
		.index = idx,
	};
	int err = iterator_next(&mi->stack[idx], &rec);
	if (err < 0)
		return err;

	if (err > 0) {
		reftable_iterator_destroy(&mi->stack[idx]);
		reftable_record_destroy(&rec);
		return 0;
	}

	merged_iter_pqueue_add(&mi->pq, e);
	return 0;
}

static int merged_iter_advance_subiter(struct merged_iter *mi, size_t idx)
{
	if (iterator_is_null(&mi->stack[idx]))
		return 0;
	return merged_iter_advance_nonnull_subiter(mi, idx);
}

static int merged_iter_next_entry(struct merged_iter *mi,
				  struct reftable_record *rec)
{
	struct strbuf entry_key = STRBUF_INIT;
	struct pq_entry entry = { 0 };
	int err = 0;

	if (merged_iter_pqueue_is_empty(mi->pq))
		return 1;

	entry = merged_iter_pqueue_remove(&mi->pq);
	err = merged_iter_advance_subiter(mi, entry.index);
	if (err < 0)
		return err;

	/*
	  One can also use reftable as datacenter-local storage, where the ref
	  database is maintained in globally consistent database (eg.
	  CockroachDB or Spanner). In this scenario, replication delays together
	  with compaction may cause newer tables to contain older entries. In
	  such a deployment, the loop below must be changed to collect all
	  entries for the same key, and return new the newest one.
	*/
	reftable_record_key(&entry.rec, &entry_key);
	while (!merged_iter_pqueue_is_empty(mi->pq)) {
		struct pq_entry top = merged_iter_pqueue_top(mi->pq);
		struct strbuf k = STRBUF_INIT;
		int err = 0, cmp = 0;

		reftable_record_key(&top.rec, &k);

		cmp = strbuf_cmp(&k, &entry_key);
		strbuf_release(&k);

		if (cmp > 0) {
			break;
		}

		merged_iter_pqueue_remove(&mi->pq);
		err = merged_iter_advance_subiter(mi, top.index);
		if (err < 0) {
			return err;
		}
		reftable_record_destroy(&top.rec);
	}

	reftable_record_copy_from(rec, &entry.rec, hash_size(mi->hash_id));
	reftable_record_destroy(&entry.rec);
	strbuf_release(&entry_key);
	return 0;
}

static int merged_iter_next(struct merged_iter *mi, struct reftable_record *rec)
{
	while (1) {
		int err = merged_iter_next_entry(mi, rec);
		if (err == 0 && mi->suppress_deletions &&
		    reftable_record_is_deletion(rec)) {
			continue;
		}

		return err;
	}
}

static int merged_iter_next_void(void *p, struct reftable_record *rec)
{
	struct merged_iter *mi = (struct merged_iter *)p;
	if (merged_iter_pqueue_is_empty(mi->pq))
		return 1;

	return merged_iter_next(mi, rec);
}

static struct reftable_iterator_vtable merged_iter_vtable = {
	.next = &merged_iter_next_void,
	.close = &merged_iter_close,
};

static void iterator_from_merged_iter(struct reftable_iterator *it,
				      struct merged_iter *mi)
{
	assert(it->ops == NULL);
	it->iter_arg = mi;
	it->ops = &merged_iter_vtable;
}

int reftable_new_merged_table(struct reftable_merged_table **dest,
			      struct reftable_table *stack, int n,
			      uint32_t hash_id)
{
	struct reftable_merged_table *m = NULL;
	uint64_t last_max = 0;
	uint64_t first_min = 0;
	int i = 0;
	for (i = 0; i < n; i++) {
		uint64_t min = reftable_table_min_update_index(&stack[i]);
		uint64_t max = reftable_table_max_update_index(&stack[i]);

		if (reftable_table_hash_id(&stack[i]) != hash_id) {
			return REFTABLE_FORMAT_ERROR;
		}
		if (i == 0 || min < first_min) {
			first_min = min;
		}
		if (i == 0 || max > last_max) {
			last_max = max;
		}
	}

	m = (struct reftable_merged_table *)reftable_calloc(
		sizeof(struct reftable_merged_table));
	m->stack = stack;
	m->stack_len = n;
	m->min = first_min;
	m->max = last_max;
	m->hash_id = hash_id;
	*dest = m;
	return 0;
}

/* clears the list of subtable, without affecting the readers themselves. */
void merged_table_release(struct reftable_merged_table *mt)
{
	FREE_AND_NULL(mt->stack);
	mt->stack_len = 0;
}

void reftable_merged_table_free(struct reftable_merged_table *mt)
{
	if (mt == NULL) {
		return;
	}
	merged_table_release(mt);
	reftable_free(mt);
}

uint64_t
reftable_merged_table_max_update_index(struct reftable_merged_table *mt)
{
	return mt->max;
}

uint64_t
reftable_merged_table_min_update_index(struct reftable_merged_table *mt)
{
	return mt->min;
}

static int reftable_table_seek_record(struct reftable_table *tab,
				      struct reftable_iterator *it,
				      struct reftable_record *rec)
{
	return tab->ops->seek_record(tab->table_arg, it, rec);
}

static int merged_table_seek_record(struct reftable_merged_table *mt,
				    struct reftable_iterator *it,
				    struct reftable_record *rec)
{
	struct reftable_iterator *iters = reftable_calloc(
		sizeof(struct reftable_iterator) * mt->stack_len);
	struct merged_iter merged = {
		.stack = iters,
		.typ = reftable_record_type(rec),
		.hash_id = mt->hash_id,
		.suppress_deletions = mt->suppress_deletions,
	};
	int n = 0;
	int err = 0;
	int i = 0;
	for (i = 0; i < mt->stack_len && err == 0; i++) {
		int e = reftable_table_seek_record(&mt->stack[i], &iters[n],
						   rec);
		if (e < 0) {
			err = e;
		}
		if (e == 0) {
			n++;
		}
	}
	if (err < 0) {
		int i = 0;
		for (i = 0; i < n; i++) {
			reftable_iterator_destroy(&iters[i]);
		}
		reftable_free(iters);
		return err;
	}

	merged.stack_len = n;
	err = merged_iter_init(&merged);
	if (err < 0) {
		merged_iter_close(&merged);
		return err;
	} else {
		struct merged_iter *p =
			reftable_malloc(sizeof(struct merged_iter));
		*p = merged;
		iterator_from_merged_iter(it, p);
	}
	return 0;
}

int reftable_merged_table_seek_ref(struct reftable_merged_table *mt,
				   struct reftable_iterator *it,
				   const char *name)
{
	struct reftable_ref_record ref = {
		.refname = (char *)name,
	};
	struct reftable_record rec = { NULL };
	reftable_record_from_ref(&rec, &ref);
	return merged_table_seek_record(mt, it, &rec);
}

int reftable_merged_table_seek_log_at(struct reftable_merged_table *mt,
				      struct reftable_iterator *it,
				      const char *name, uint64_t update_index)
{
	struct reftable_log_record log = {
		.refname = (char *)name,
		.update_index = update_index,
	};
	struct reftable_record rec = { NULL };
	reftable_record_from_log(&rec, &log);
	return merged_table_seek_record(mt, it, &rec);
}

int reftable_merged_table_seek_log(struct reftable_merged_table *mt,
				   struct reftable_iterator *it,
				   const char *name)
{
	uint64_t max = ~((uint64_t)0);
	return reftable_merged_table_seek_log_at(mt, it, name, max);
}

uint32_t reftable_merged_table_hash_id(struct reftable_merged_table *mt)
{
	return mt->hash_id;
}

static int reftable_merged_table_seek_void(void *tab,
					   struct reftable_iterator *it,
					   struct reftable_record *rec)
{
	return merged_table_seek_record((struct reftable_merged_table *)tab, it,
					rec);
}

static uint32_t reftable_merged_table_hash_id_void(void *tab)
{
	return reftable_merged_table_hash_id(
		(struct reftable_merged_table *)tab);
}

static uint64_t reftable_merged_table_min_update_index_void(void *tab)
{
	return reftable_merged_table_min_update_index(
		(struct reftable_merged_table *)tab);
}

static uint64_t reftable_merged_table_max_update_index_void(void *tab)
{
	return reftable_merged_table_max_update_index(
		(struct reftable_merged_table *)tab);
}

static struct reftable_table_vtable merged_table_vtable = {
	.seek_record = reftable_merged_table_seek_void,
	.hash_id = reftable_merged_table_hash_id_void,
	.min_update_index = reftable_merged_table_min_update_index_void,
	.max_update_index = reftable_merged_table_max_update_index_void,
};

void reftable_table_from_merged_table(struct reftable_table *tab,
				      struct reftable_merged_table *merged)
{
	assert(tab->ops == NULL);
	tab->ops = &merged_table_vtable;
	tab->table_arg = merged;
}
