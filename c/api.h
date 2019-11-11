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

#ifndef API_H
#define API_H

#include "basics.h"
#include "constants.h"
#include "slice.h"

typedef struct _block block;

/* block_source_ops are the operations that make up block_source */
typedef struct {
  uint64 (*size)(void *source);
  int (*read_block)(void *source, block *dest, uint64 off, uint32 size);
  void (*return_block)(void *source, block *blockp);
  void (*close)(void *source);
} block_source_ops;

/* block_source is a generic wrapper for a seekable readable file. */
typedef struct _block_source block_source;

/* block_source is a value type. */
struct _block_source {
  block_source_ops *ops;
  void *arg;
};

struct _block {
  byte *data;
  int len;
  block_source source;
};

uint64 block_source_size(block_source source);
int block_source_read_block(block_source source, block *dest, uint64 off,
                            uint32 size);
void block_source_return_block(block_source source, block *ret);
void block_source_close(block_source source);

/* write_options sets optiosn for writing a single reftable. */
typedef struct {
  bool unpadded;
  uint32 block_size;
  uint32 min_update_index;
  uint32 max_update_index;
  bool skip_index_objects;
  int restart_interval;
} write_options;

/* ref_record holds a ref database entry target_value */
typedef struct {
  char *ref_name; // name of the ref. Must be specified.
  uint64 update_index;
  byte *value; // SHA1, or NULL
  byte *target_value; // peeled annotated tag, or NULL.
  char *target; // symref, or NULL
} ref_record;

void ref_record_clear(ref_record *ref);

/* log_record holds a reflog entry */
typedef struct {
  char *ref_name;
  uint64 update_index;
  char *new_hash;
  char *old_hash;
  char *name;
  char *email;
  uint64 time;
  uint64 tz_offset;
  char *message;
} log_record;

/* iterator is the generic interface for walking over data stored in a
   reftable. */
typedef struct {
  struct _iterator_ops *ops;
  void *iter_arg;
} iterator;

// < 0: error, 0 = OK, > 0: end of iteration
int iterator_next_ref(iterator it, ref_record *ref);

// iterator_destroy must be called after finishing an iteration.
void iterator_destroy(iterator *it);

/* block_stats holds statistics for a single block type */
typedef struct {
  int entries;
  int restarts;
  int blocks;
  int index_blocks;
  int max_index_level;

  uint64 offset;
  uint64 index_offset;
} block_stats;

/* stats holds overall statistics for a single reftable */
typedef struct {
  int blocks;
  block_stats ref_stats;
  block_stats obj_stats;
  block_stats idx_stats;
  // todo: log stats.
  int object_id_len;
} stats;

#define IO_ERROR -2
#define FORMAT_ERROR -3

typedef struct _writer writer;

/* new_writer creates a new writer */
writer *new_writer(int (*writer_func)(void *, byte *, int), void *writer_arg,
                   write_options *opts);

/* writer_add_ref adds a ref_record. Must be called in ascending order. */
int writer_add_ref(writer *w, ref_record *ref);

/* writer_close finalizes the reftable. The writer is retained so statistics can be inspected. */
int writer_close(writer *w);

/* writer_stats returns the statistics on the reftable being written. */
stats* writer_stats(writer *w);

/* writer_free deallocates memory for the writer */
void writer_free(writer *w);

typedef struct _reader reader;

/* new_reader opens a reftable for reading. If successful, returns 0 code and sets pp */
int new_reader(reader **pp, block_source);

/* reader_seek_ref returns an iterator where 'name' would be inserted in the table.

   example:

   reader *r  = NULL;
   int err = new_reader(&r, src);
   if (err < 0) { ... }
   iterator it = {};
   err = reader_seek_ref(r, &it, "refs/heads/master");
   if (err < 0) { ... }
 */
int reader_seek_ref(reader *r, iterator *it, char *name);
void reader_free(reader *);
int reader_refs_for(reader* r, iterator *it, byte *oid);

#endif
