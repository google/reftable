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

/* block_source is a generic wrapper for a seekable readable file. */
struct block_source {
  struct block_source_ops *ops;
  void *arg;
};

struct block {
  byte *data;
  int len;
  struct block_source source;
};

/* block_source_ops are the operations that make up block_source */
struct block_source_ops {
  uint64_t (*size)(void *source);
  int (*read_block)(void *source, struct block *dest, uint64_t off,
                    uint32_t size);
  void (*return_block)(void *source, struct block *blockp);
  void (*close)(void *source);
};

uint64_t block_source_size(struct block_source source);
int block_source_read_block(struct block_source source, struct block *dest,
                            uint64_t off, uint32_t size);
void block_source_return_block(struct block_source source, struct block *ret);
void block_source_close(struct block_source source);
int block_source_from_file(struct block_source *block_src, const char *name);

/* write_options sets optiosn for writing a single reftable. */
struct write_options {
  bool unpadded;
  uint32_t block_size;
  bool skip_index_objects;
  int restart_interval;

  // TODO - move this to the writer API
  uint32_t min_update_index;
  uint32_t max_update_index;
};

/* ref_record holds a ref database entry target_value */
struct ref_record {
  char *ref_name;  // name of the ref. Must be specified.
  uint64_t update_index;
  byte *value;         // SHA1, or NULL
  byte *target_value;  // peeled annotated tag, or NULL.
  char *target;        // symref, or NULL
};

void ref_record_print(struct ref_record *ref);
void ref_record_clear(struct ref_record *ref);
bool ref_record_equal(struct ref_record *a, struct ref_record *b);

/* log_record holds a reflog entry */
struct log_record {
  char *ref_name;
  uint64_t update_index;
  char *new_hash;
  char *old_hash;
  char *name;
  char *email;
  uint64_t time;
  uint64_t tz_offset;
  char *message;
};

/* iterator is the generic interface for walking over data stored in a
   reftable. */
struct iterator {
  struct iterator_ops *ops;
  void *iter_arg;
};

// < 0: error, 0 = OK, > 0: end of iteration
int iterator_next_ref(struct iterator it, struct ref_record *ref);

// iterator_destroy must be called after finishing an iteration.
void iterator_destroy(struct iterator *it);

/* block_stats holds statistics for a single block type */
struct block_stats {
  int entries;
  int restarts;
  int blocks;
  int index_blocks;
  int max_index_level;

  uint64_t offset;
  uint64_t index_offset;
};

/* stats holds overall statistics for a single reftable */
struct stats {
  int blocks;
  struct block_stats ref_stats;
  struct block_stats obj_stats;
  struct block_stats idx_stats;

  int object_id_len;
};

// different types of errors
#define IO_ERROR -2
#define FORMAT_ERROR -3
#define ERR_NOT_EXIST -4
#define LOCK_FAILURE -5

/* new_writer creates a new writer */
struct writer *new_writer(int (*writer_func)(void *, byte *, int),
                          void *writer_arg, struct write_options *opts);

/* write to a file descriptor. fdp should be an int* pointing to the fd. */
int fd_writer(void* fdp, byte*data, int size);

/* writer_add_ref adds a ref_record. Must be called in ascending order. */
int writer_add_ref(struct writer *w, struct ref_record *ref);

/* writer_close finalizes the reftable. The writer is retained so statistics can
 * be inspected. */
int writer_close(struct writer *w);

/* writer_stats returns the statistics on the reftable being written. */
struct stats *writer_stats(struct writer *w);

/* writer_free deallocates memory for the writer */
void writer_free(struct writer *w);

struct reader;

/* new_reader opens a reftable for reading. If successful, returns 0 code and
 * sets pp */
int new_reader(struct reader **pp, struct block_source, const char *name);

/* reader_seek_ref returns an iterator where 'name' would be inserted in the
   table.

   example:

   struct reader *r = NULL;
   int err = new_reader(&r, src, "filename");
   if (err < 0) { ... }
   iterator it = {};
   err = reader_seek_ref(r, &it, "refs/heads/master");
   if (err < 0) { ... }
   ref_record ref = {};
   err = iterator_next_ref(it, &ref);
   if (err == 0) {
      // value found.
   }
   iterator_destroy(&it);

 */
int reader_seek_ref(struct reader *r, struct iterator *it, char *name);
void reader_free(struct reader *);
int reader_refs_for(struct reader *r, struct iterator *it, byte *oid);
uint64_t reader_max_update_index(struct reader *r);
uint64_t reader_min_update_index(struct reader *r);

struct merged_table;
int new_merged_table(struct merged_table **dest, struct reader **stack, int n);
int merged_table_seek_ref(struct merged_table *mt, struct iterator *it,
                          struct ref_record *ref);
void merged_table_free(struct merged_table *m);

#endif
