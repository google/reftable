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

#ifndef MERGED_H
#define MERGED_H

#include "api.h"
#include "pq.h"

struct merged_table {
  struct reader **stack;
  int stack_len;

  uint64_t min;
  uint64_t max;
};

struct merged_iter {
  struct iterator *stack;
  int stack_len;
  byte typ;
  struct merged_iter_pqueue pq;
} merged_iter;

void merged_table_clear(struct merged_table *mt);

#endif
