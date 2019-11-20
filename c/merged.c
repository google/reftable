// Copyright 2019 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdlib.h>

#include "iter.h"
#include "pq.h"
#include "reader.h"

struct _merged_table {
  reader **stack;
  int stack_len;

  uint64_t min;
  uint64_t max;
};

typedef struct {
  iterator *stack;
  int stack_len;
  byte typ;
  merged_iter_pqueue pq;
} merged_iter;

int merged_iter_init(merged_iter *mi) {
  for (int i = 0; i < mi->stack_len; i++) {
    record rec = new_record(mi->typ);
    int err = iterator_next(mi->stack[i], rec);
    if (err < 0) {
      return err;
    }

    if (err > 0) {
      iterator_destroy(&mi->stack[i]);
    } else {
      pq_entry e = {
          .rec = rec,
          .index = i,
      };
      merged_iter_pqueue_add(&mi->pq, e);
    }
  }

  return 0;
}

void merged_iter_close(void *p) {
  merged_iter *mi = (merged_iter *)p;

  merged_iter_pqueue_clear(&mi->pq);
  for (int i = 0; i < mi->stack_len; i++) {
    iterator_destroy(&mi->stack[i]);
  }
}

int merged_iter_advance_subiter(merged_iter *mi, int idx) {
  if (iterator_is_null(mi->stack[idx])) {
    return 0;
  }

  record rec = new_record(mi->typ);
  int err = iterator_next(mi->stack[idx], rec);
  if (err < 0) {
    return err;
  }

  if (err > 0) {
    iterator_destroy(&mi->stack[idx]);
    return 0;
  }

  pq_entry e = {
      .rec = rec,
      .index = idx,
  };
  merged_iter_pqueue_add(&mi->pq, e);
  return 0;
}

int merged_iter_next(void *p, record rec) {
  merged_iter *mi = (merged_iter *)p;
  if (merged_iter_pqueue_is_empty(mi->pq)) {
    return 1;
  }

  pq_entry entry = merged_iter_pqueue_remove(&mi->pq);
  int err = merged_iter_advance_subiter(mi, entry.index);
  if (err < 0) {
    return err;
  }

  slice entry_key = {};
  record_key(entry.rec, &entry_key);
  while (!merged_iter_pqueue_is_empty(mi->pq)) {
    pq_entry top = merged_iter_pqueue_top(mi->pq);

    slice k = {};
    record_key(top.rec, &k);

    int cmp = slice_compare(k, entry_key);
    free(slice_yield(&k));

    if (cmp > 0) {
      break;
    }

    merged_iter_pqueue_remove(&mi->pq);
    int err = merged_iter_advance_subiter(mi, top.index);
    if (err < 0) {
      return err;
    }
    record_clear(top.rec);
    free(record_yield(&top.rec));
  }

  record_copy_from(rec, entry.rec);
  record_clear(entry.rec);
  free(record_yield(&entry.rec));
  free(slice_yield(&entry_key));
  return 0;
}

struct _iterator_ops merged_iter_ops = {
    .next = &merged_iter_next,
    .close = &merged_iter_close,
};

void iterator_from_merged_iter(iterator *it, merged_iter *mi) {
  it->iter_arg = mi;
  it->ops = &merged_iter_ops;
}

/* new_merged_table creates a new merged table. It takes ownership of the stack
 * array. */
int new_merged_table(merged_table **dest, reader **stack, int n) {
  uint64_t last_max = 0;
  uint64_t first_min = 0;
  for (int i = 0; i < n; i++) {
    reader *r = stack[i];
    if (i > 0 && last_max >= reader_min_update_index(r)) {
      return FORMAT_ERROR;
    }
    if (i == 0) {
      first_min = reader_min_update_index(r);
    }

    last_max = reader_max_update_index(r);
  }

  merged_table m = {
      .stack = stack,
      .stack_len = n,
      .min = first_min,
      .max = last_max,
  };

  *dest = calloc(sizeof(merged_table), 1);
  **dest = m;
  return 0;
}

uint64_t merged_max_update_index(merged_table *mt) { return mt->max; }

uint64_t merged_min_update_index(merged_table *mt) { return mt->min; }

int merged_table_seek_record(merged_table *mt, iterator *it, record rec) {
  iterator *iters = calloc(sizeof(iterator), mt->stack_len);
  for (int i = 0; i < mt->stack_len; i++) {
    int err = reader_seek(mt->stack[i], &iters[i], rec);
    if (err < 0) {
      // XXX leak.
      return err;
    }
  }

  merged_iter merged = {
      .stack = iters,
      .stack_len = mt->stack_len,
      .typ = record_type(rec),
  };

  int err = merged_iter_init(&merged);
  if (err < 0) {
    return err;
  }

  merged_iter *p = malloc(sizeof(merged_iter));
  *p = merged;
  iterator_from_merged_iter(it, p);
  return 0;
}

int merged_table_seek_ref(merged_table *mt, iterator *it, ref_record *ref) {
  record rec = {};
  record_from_ref(&rec, ref);
  return merged_table_seek_record(mt, it, rec);
}
