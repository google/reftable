/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#ifndef MERGED_H
#define MERGED_H

#include "pq.h"
#include "reftable.h"

struct reftable_merged_table {
	struct reftable_reader **stack;
	int stack_len;
	uint32_t hash_id;
	bool suppress_deletions;

	uint64_t min;
	uint64_t max;
};

struct merged_iter {
	struct reftable_iterator *stack;
	uint32_t hash_id;
	int stack_len;
	byte typ;
	bool suppress_deletions;
	struct merged_iter_pqueue pq;
};

void merged_table_clear(struct reftable_merged_table *mt);
int merged_table_seek_record(struct reftable_merged_table *mt,
			     struct reftable_iterator *it,
			     struct reftable_record *rec);

#endif
