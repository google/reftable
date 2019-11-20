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

#ifndef RECORD_H
#define RECORD_H

#include "api.h"
#include "slice.h"

typedef struct _record_ops {
  void (*key)(const void *rec, slice *dest);
  byte (*type)();
  void (*copy_from)(void *rec, const void *src);
  byte (*val_type)(const void *rec);
  int (*encode)(const void *rec, slice dest);
  int (*decode)(void *rec, slice key, byte extra, slice src);
  void (*clear)(void *rec);
} record_ops;

/* record is a generic wrapper for differnt types of records. */
typedef struct {
  void *data;
  struct _record_ops *ops;
} record;

int get_var_int(uint64_t *dest, slice in);
int put_var_int(slice dest, uint64_t val);
int common_prefix_size(slice a, slice b);

int is_block_type(byte typ);
record new_record(byte typ);

extern record_ops ref_record_ops;

int encode_key(bool *restart, slice dest, slice prev_key, slice key,
               byte extra);
int decode_key(slice *key, byte *extra, slice last_key, slice in);

typedef struct {
  slice last_key;
  uint64_t offset;
} index_record;

typedef struct {
  byte *hash_prefix;
  int hash_prefix_len;
  uint64_t *offsets;
  int offset_len;
} obj_record;

void record_key(record rec, slice *dest);
byte record_type(record rec);
void record_copy_from(record rec, record src);
byte record_val_type(record rec);
int record_encode(record rec, slice dest);
int record_decode(record rec, slice key, byte extra, slice src);
void record_clear(record rec);
void *record_yield(record *rec);
void record_from_obj(record *rec, obj_record *objrec);
void record_from_index(record *rec, index_record *idxrec);
void record_from_ref(record *rec, ref_record *refrec);
void record_from_log(record *rec, log_record *objrec);
ref_record *record_as_ref(record ref);

bool record_is_start(record want);

#endif
