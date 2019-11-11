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

#include "api.h"

typedef struct {
  bool present;
  uint64 offset;
  uint64 index_offset;
} reader_offsets;

struct _reader {
  block_source source;

  uint64 size;
  uint32 block_size;
  uint64 min_update_index;
  uint64 max_update_index;
  int object_id_len;

  reader_offsets ref_offsets;
  reader_offsets obj_offsets;
  reader_offsets log_offsets;
};

int init_reader(reader *r, block_source source);
int reader_seek(reader *r, iterator *it, record rec);
void reader_close(reader *r);
void reader_return_block(reader *r, block *p);
int reader_init_block_reader(reader *r, block_reader *br, uint64 next_off,
                             byte want_typ);

#endif
