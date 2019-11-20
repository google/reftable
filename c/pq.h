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

struct pq_entry {
  struct record rec;
  int index;
};

int pq_less(struct pq_entry a, struct pq_entry b);

struct merged_iter_pqueue {
  struct pq_entry *heap;
  int len;
  int cap;
};

struct pq_entry merged_iter_pqueue_top(struct merged_iter_pqueue pq);
bool merged_iter_pqueue_is_empty(struct merged_iter_pqueue pq);
void merged_iter_pqueue_check(struct merged_iter_pqueue pq);
struct pq_entry merged_iter_pqueue_remove(struct merged_iter_pqueue *pq);
void merged_iter_pqueue_add(struct merged_iter_pqueue *pq, struct pq_entry e);
void merged_iter_pqueue_clear(struct merged_iter_pqueue *pq);

#endif
