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

#ifndef ITER_H
#define ITER_H

#include "block.h"
#include "record.h"

struct iterator_ops {
  int (*next)(void *iter_arg, struct record rec);
  void (*close)(void *iter_arg);
};

void iterator_set_empty(struct iterator *it);
int iterator_next(struct iterator it, struct record rec);
bool iterator_is_null(struct iterator it);

struct filtering_ref_iterator {
  struct reader *r;
  byte *oid;
  bool double_check;
  struct iterator it;
};

void iterator_from_filtering_ref_iterator(struct iterator *,
                                          struct filtering_ref_iterator *);

struct indexed_table_ref_iter {
  struct reader *r;
  byte *oid;

  // mutable
  uint64_t *offsets;

  // Points to the next offset to read.
  int offset_idx;
  int offset_len;
  struct block_reader block_reader;
  struct block_iter cur;
  bool finished;
};

void iterator_from_indexed_table_ref_iter(struct iterator *it,
                                          struct indexed_table_ref_iter *itr);
int new_indexed_table_ref_iter(struct indexed_table_ref_iter **dest,
                               struct reader *r, byte *oid, uint64_t *offsets,
                               int offset_len);

#endif
