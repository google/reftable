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

#ifndef STACK_H
#define STACK_H

#include "api.h"

struct compaction_stats {
  uint64_t bytes;
  int attempts;
  int failures;
};

struct stack {
  char *list_file;
  char *reftable_dir;
  // XXX config
  struct write_options cfg;

  struct merged_table *merged;
  struct compaction_stats stats;
};

int read_lines(const char* filename, char ***lines);
int stack_try_add(struct stack* st, int (*write_table)(struct writer *wr, void*arg), void *arg);
int stack_write_compact(struct stack *st, struct writer *wr, int first, int last);
int fastlog2(uint64_t sz);

struct segment {
  int start, end;
  int log;
  uint64_t bytes;
};

struct segment *sizes_to_segments(int *seglen, uint64_t *sizes, int n);
struct segment suggest_compaction_segment(uint64_t*sizes, int n);

#endif
   
