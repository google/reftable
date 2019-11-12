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

#include "record.h"
#include "block.h"

struct _iterator_ops {
  int (*next)(void *iter_arg, record rec);
  void (*close)(void *iter_arg);
};

void iterator_set_empty(iterator *it);
int iterator_next(iterator it, record rec);
bool iterator_is_null(iterator it);

typedef struct {
  reader *r;
  byte *oid ;
  bool double_check ;
  iterator it;
} filtering_ref_iterator;

void iterator_from_filtering_ref_iterator(iterator*, filtering_ref_iterator*);

typedef struct {
  reader *r;
  byte *oid;

  // mutable
  uint64 *offsets;

  // Points to the next offset to read.
  int offset_idx;
  int offset_len;
  block_reader block_reader;
  block_iter cur;
  bool finished;
} indexed_table_ref_iter;

void iterator_from_indexed_table_ref_iter(iterator *it, indexed_table_ref_iter *itr);
int new_indexed_table_ref_iter(indexed_table_ref_iter **dest,
			       reader * r,
			       byte *oid,
			       uint64 *offsets,
			       int offset_len);

#endif
