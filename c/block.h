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

#ifndef BLOCK_H
#define BLOCK_H

#include "api.h"
#include "basics.h"
#include "record.h"

struct block_writer {
  byte *buf;
  uint32_t block_size;
  uint32_t header_off;
  int restart_interval;

  uint32_t next;
  uint32_t *restarts;
  uint32_t restart_len;
  uint32_t restart_cap;
  struct slice last_key;
  int entries;
};

void block_writer_init(struct block_writer *bw, byte typ, byte *buf,
                       uint32_t block_size, uint32_t header_off);
byte block_writer_type(struct block_writer *bw);
int block_writer_add(struct block_writer *w, struct record rec);
int block_writer_finish(struct block_writer *w);
void block_writer_reset(struct block_writer *bw);
void block_writer_clear(struct block_writer *bw);

struct block_reader {
  uint32_t header_off;
  struct block block;

  // size of the data, excluding restart data.
  uint32_t block_len;
  byte *restart_bytes;
  uint32_t full_block_size;
  uint16_t restart_count;
};

struct block_iter {
  struct block_reader *br;
  struct slice last_key;
  uint32_t next_off;
};

int block_reader_init(struct block_reader *br, struct block *bl,
                      uint32_t header_off, uint32_t table_block_size);
void block_reader_start(struct block_reader *br, struct block_iter *it);
int block_reader_seek(struct block_reader *br, struct block_iter *it,
                      struct slice want);
byte block_reader_type(struct block_reader *r);
int block_reader_first_key(struct block_reader *br, struct slice *key);

void block_iter_copy_from(struct block_iter *dest, struct block_iter *src);
int block_iter_next(struct block_iter *it, struct record rec);
int block_iter_seek(struct block_iter *it, struct slice want);
void block_iter_close(struct block_iter *it);

#endif
