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

#include "reftable.h"
#include "slice.h"

struct record_vtable {
  void (*key)(const void *rec, struct slice *dest);
  byte (*type)(void);
  void (*copy_from)(void *rec, const void *src, int hash_size);
  byte (*val_type)(const void *rec);
  int (*encode)(const void *rec, struct slice dest, int hash_size);
  int (*decode)(void *rec, struct slice key, byte extra, struct slice src, int hash_size);
  void (*clear)(void *rec);
};

/* record is a generic wrapper for differnt types of records. */
struct record {
  void *data;
  struct record_vtable *ops;
};

int get_var_int(uint64_t *dest, struct slice in);
int put_var_int(struct slice dest, uint64_t val);
int common_prefix_size(struct slice a, struct slice b);

int is_block_type(byte typ);
struct record new_record(byte typ);

extern struct record_vtable ref_record_vtable;

int encode_key(bool *restart, struct slice dest, struct slice prev_key,
               struct slice key, byte extra);
int decode_key(struct slice *key, byte *extra, struct slice last_key,
               struct slice in);

struct index_record {
  struct slice last_key;
  uint64_t offset;
};

struct obj_record {
  byte *hash_prefix;
  int hash_prefix_len;
  uint64_t *offsets;
  int offset_len;
};

void record_key(struct record rec, struct slice *dest);
byte record_type(struct record rec);
void record_copy_from(struct record rec, struct record src, int hash_size);
byte record_val_type(struct record rec);
int record_encode(struct record rec, struct slice dest, int hash_size);
int record_decode(struct record rec, struct slice key, byte extra,
                  struct slice src, int hash_size);
void record_clear(struct record rec);
void *record_yield(struct record *rec);
void record_from_obj(struct record *rec, struct obj_record *objrec);
void record_from_index(struct record *rec, struct index_record *idxrec);
void record_from_ref(struct record *rec, struct ref_record *refrec);
void record_from_log(struct record *rec, struct log_record *logrec);
struct ref_record *record_as_ref(struct record ref);

#endif
