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

#ifndef PQ_H
#define PQ_H

#include "record.h"

typedef struct {
  record rec;
  int index;
} pq_entry;

int pq_less(pq_entry a, pq_entry b);

typedef struct {
  pq_entry *heap;
  int len;
  int cap;
} merged_iter_pqueue;

pq_entry merged_iter_pqueue_top(merged_iter_pqueue pq);
bool merged_iter_pqueue_is_empty(merged_iter_pqueue pq);
void merged_iter_pqueue_check(merged_iter_pqueue pq);
pq_entry merged_iter_pqueue_remove(merged_iter_pqueue *pq);
void merged_iter_pqueue_add(merged_iter_pqueue *pq, pq_entry e);
void merged_iter_pqueue_clear(merged_iter_pqueue *pq);

#endif
