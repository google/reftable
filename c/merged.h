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

struct merged_table {
	struct reader **stack;
	int stack_len;
	uint32_t hash_id;

	uint64_t min;
	uint64_t max;
};

struct merged_iter {
	struct iterator *stack;
	uint32_t hash_id;
	int stack_len;
	byte typ;
	struct merged_iter_pqueue pq;
} merged_iter;

void merged_table_clear(struct merged_table *mt);

#endif
