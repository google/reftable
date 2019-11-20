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

#ifndef WRITER_H
#define WRITER_H

#include "api.h"
#include "basics.h"
#include "block.h"
#include "slice.h"
#include "tree.h"

struct writer {
  int (*write)(void *, byte *, int);
  void *write_arg;
  int pending_padding;

  struct slice last_key;

  uint64_t next;
  struct write_options opts;

  byte *block;
  struct block_writer *block_writer;
  struct block_writer block_writer_data;
  struct index_record *index;
  int index_len;
  int index_cap;

  // tree for use with tsearch
  struct tree_node *obj_index_tree;

  struct stats stats;
};

int writer_flush_block(struct writer *w);
void writer_clear_index(struct writer *w);

#endif
