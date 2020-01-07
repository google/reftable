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

#ifndef READER_H
#define READER_H

#include "block.h"
#include "record.h"
#include "reftable.h"

uint64_t block_source_size(struct block_source source);

int block_source_read_block(struct block_source source, struct block *dest,
                            uint64_t off, uint32_t size);
void block_source_return_block(struct block_source source, struct block *ret);
void block_source_close(struct block_source *source);

struct reader_offsets {
  bool present;
  uint64_t offset;
  uint64_t index_offset;
};

struct reader {
  struct block_source source;
  char *name;
  int hash_size;
  uint64_t size;
  uint32_t block_size;
  uint64_t min_update_index;
  uint64_t max_update_index;
  int object_id_len;

  struct reader_offsets ref_offsets;
  struct reader_offsets obj_offsets;
  struct reader_offsets log_offsets;
};

int init_reader(struct reader *r, struct block_source source, const char *name);
int reader_seek(struct reader *r, struct iterator *it, struct record rec);
void reader_close(struct reader *r);
const char *reader_name(struct reader *r);
void reader_return_block(struct reader *r, struct block *p);
int reader_init_block_reader(struct reader *r, struct block_reader *br,
                             uint64_t next_off, byte want_typ);

#endif
