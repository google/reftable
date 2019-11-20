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

#include "writer.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>  // debug
#include <stdlib.h>
#include <string.h>

#include "api.h"
#include "block.h"
#include "record.h"
#include "tree.h"

block_stats *writer_block_stats(writer *w, byte typ) {
  switch (typ) {
    case 'r':
      return &w->stats.ref_stats;
    case 'o':
      return &w->stats.obj_stats;
    case 'i':
      return &w->stats.idx_stats;
  }
  assert(false);
}

int padded_write(writer *w, slice out, int padding) {
  if (w->pending_padding > 0) {
    byte *zeroed = calloc(w->pending_padding, 1);
    int n = w->write(w->write_arg, zeroed, w->pending_padding);
    if (n < 0) {
      return n;
    }

    w->pending_padding = 0;
    free(zeroed);
  }

  w->pending_padding = padding;
  int n = w->write(w->write_arg, out.buf, out.len);
  if (n < 0) {
    return n;
  }
  n += padding;
  return n;
}

void options_set_defaults(write_options *opts) {
  if (opts->restart_interval == 0) {
    opts->restart_interval = 16;
  }

  if (opts->block_size == 0) {
    opts->block_size = DEFAULT_BLOCK_SIZE;
  }
}

int writer_write_header(writer *w, byte *dest) {
  strcpy((char *)dest, "REFT");
  dest[4] = 1;  // version
  put_u24(dest + 5, w->opts.block_size);
  put_u64(dest + 8, w->opts.min_update_index);
  put_u64(dest + 16, w->opts.max_update_index);
  return 24;
}

void writer_reinit_block_writer(writer *w, byte typ) {
  int block_start = 0;
  if (w->next == 0) {
    block_start = writer_write_header(w, w->block);
  }

  block_writer_init(&w->block_writer_data, typ, w->block, w->opts.block_size,
                    block_start);
  w->block_writer = &w->block_writer_data;
  w->block_writer->restart_interval = w->opts.restart_interval;
}

writer *new_writer(int (*writer_func)(void *, byte *, int), void *writer_arg,
                   write_options *opts) {
  options_set_defaults(opts);
  if (opts->block_size >= (1 << 24)) {
    abort();
  }
  writer *wp = calloc(sizeof(writer), 1);
  wp->block = calloc(opts->block_size, 1);
  wp->write = writer_func;
  wp->write_arg = writer_arg;
  wp->opts = *opts;
  writer_reinit_block_writer(wp, BLOCK_TYPE_REF);

  return wp;
}

void writer_free(writer *w) {
  free(w->block);
  free(w);
}

typedef struct {
  slice hash;
  uint64_t *offsets;
  int offset_len;
  int offset_cap;
} obj_index_tree_node;

int obj_index_tree_node_compare(const void *a, const void *b) {
  return slice_compare(((const obj_index_tree_node *)a)->hash,
                       ((const obj_index_tree_node *)b)->hash);
}

void writer_index_hash(writer *w, slice hash) {
  if (w->opts.skip_index_objects) {
    return;
  }

  uint64_t off = w->next;

  obj_index_tree_node want = {};
  slice_copy(&want.hash, hash);

  tree_node *node =
      tree_search(&want, &w->obj_index_tree, &obj_index_tree_node_compare, 0);
  obj_index_tree_node *key = NULL;
  if (node == NULL) {
    key = calloc(sizeof(obj_index_tree_node), 1);
    slice_copy(&key->hash, hash);
    tree_search((void *)key, &w->obj_index_tree, &obj_index_tree_node_compare,
                1);
  } else {
    key = node->key;
  }

  if (key->offset_len > 0 && key->offsets[key->offset_len - 1] == off) {
    return;
  }

  if (key->offset_len == key->offset_cap) {
    key->offset_cap = 2 * key->offset_cap + 1;
    key->offsets = realloc(key->offsets, sizeof(uint64_t) * key->offset_cap);
  }

  key->offsets[key->offset_len++] = off;
  free(slice_yield(&want.hash));
}

int writer_add_record(writer *w, record rec) {
  int result = -1;
  slice key = {};
  record_key(rec, &key);
  if (slice_compare(w->last_key, key) >= 0) {
    goto exit;
  }

  slice_copy(&w->last_key, key);
  byte typ = record_type(rec);
  if (w->block_writer == NULL) {
    writer_reinit_block_writer(w, typ);
  }

  assert(block_writer_type(w->block_writer) == typ);

  if (block_writer_add(w->block_writer, rec) == 0) {
    result = 0;
    goto exit;
  }

  int err = writer_flush_block(w);
  if (err < 0) {
    result = err;
    goto exit;
  }

  writer_reinit_block_writer(w, typ);
  err = block_writer_add(w->block_writer, rec);
  if (err < 0) {
    // XXX error code.
    result = err;
    goto exit;
  }

  result = 0;
exit:
  free(slice_yield(&key));
  return result;
}

int writer_add_ref(writer *w, ref_record *ref) {
  if (ref->update_index < w->opts.min_update_index ||
      ref->update_index > w->opts.max_update_index) {
    return -1;
  }

  record rec = {};
  ref_record copy = *ref;
  record_from_ref(&rec, &copy);
  copy.update_index -= w->opts.min_update_index;
  int err = writer_add_record(w, rec);
  if (err < 0) {
    return err;
  }

  if (ref->value != NULL) {
    slice h = {
        .buf = ref->value,
        .len = HASH_SIZE,
    };

    writer_index_hash(w, h);
  }
  if (ref->target_value != NULL) {
    slice h = {
        .buf = ref->target_value,
        .len = HASH_SIZE,
    };
    writer_index_hash(w, h);
  }
  return 0;
}

int writer_finish_section(writer *w) {
  w->last_key.len = 0;
  byte typ = block_writer_type(w->block_writer);
  int err = writer_flush_block(w);
  if (err < 0) {
    return err;
  }

  uint64_t index_start = 0;
  int max_level = 0;
  int threshold = 3;
  if (w->opts.unpadded) {
    threshold = 1;
  }

  int before_blocks = w->stats.idx_stats.blocks;
  while (w->index_len > threshold) {
    max_level++;
    index_start = w->next;
    writer_reinit_block_writer(w, BLOCK_TYPE_INDEX);
    index_record *idx = w->index;
    int idx_len = w->index_len;

    w->index = NULL;
    w->index_len = 0;
    w->index_cap = 0;
    for (int i = 0; i < idx_len; i++) {
      record rec = {};
      record_from_index(&rec, idx + i);
      if (block_writer_add(w->block_writer, rec) == 0) {
        continue;
      }

      int err = writer_flush_block(w);
      if (err < 0) {
        return err;
      }
      writer_reinit_block_writer(w, BLOCK_TYPE_INDEX);

      err = block_writer_add(w->block_writer, rec);
      assert(err == 0);
    }
    for (int i = 0; i < idx_len; i++) {
      free(slice_yield(&idx[i].last_key));
    }
    free(idx);
  }

  writer_clear_index(w);

  err = writer_flush_block(w);
  if (err < 0) {
    return err;
  }

  block_stats *bstats = writer_block_stats(w, typ);
  bstats->index_blocks = w->stats.idx_stats.blocks - before_blocks;
  bstats->index_offset = index_start;
  bstats->max_index_level = max_level;

  return 0;
}

typedef struct {
  slice *last;
  int max;
} common_prefix_arg;

void update_common(void *void_arg, void *key) {
  common_prefix_arg *arg = (common_prefix_arg *)void_arg;
  obj_index_tree_node *entry = (obj_index_tree_node *)key;
  if (arg->last != NULL) {
    int n = common_prefix_size(entry->hash, *arg->last);
    if (n > arg->max) {
      arg->max = n;
    }
  }
  arg->last = &entry->hash;
}

typedef struct {
  writer *w;
  int err;

} write_record_arg;

void write_object_record(void *void_arg, void *key) {
  write_record_arg *arg = (write_record_arg *)void_arg;
  obj_index_tree_node *entry = (obj_index_tree_node *)key;

  if (arg->err < 0) {
    return;
  }

  obj_record obj_rec = {
      .hash_prefix = entry->hash.buf,
      .hash_prefix_len = arg->w->stats.object_id_len,
      .offsets = entry->offsets,
      .offset_len = entry->offset_len,
  };

  record rec = {};
  record_from_obj(&rec, &obj_rec);
  int err = block_writer_add(arg->w->block_writer, rec);
  if (err == 0) {
    return;
  }

  err = writer_flush_block(arg->w);
  if (err < 0) {
    arg->err = err;
    return;
  }

  writer_reinit_block_writer(arg->w, BLOCK_TYPE_OBJ);
  err = block_writer_add(arg->w->block_writer, rec);
  if (err == 0) {
    return;
  }
  obj_rec.offset_len = 0;
  err = block_writer_add(arg->w->block_writer, rec);
  assert(err == 0);

  free(entry->offsets);
  entry->offsets = NULL;
  free(slice_yield(&entry->hash));
}

int writer_dump_object_index(writer *w) {
  common_prefix_arg common = {};
  if (w->obj_index_tree != NULL) {
    infix_walk(w->obj_index_tree, &update_common, &common);
  }
  w->stats.object_id_len = common.max + 1;

  writer_reinit_block_writer(w, BLOCK_TYPE_OBJ);

  write_record_arg closure = {.w = w};
  if (w->obj_index_tree != NULL) {
    infix_walk(w->obj_index_tree, &write_object_record, &closure);
  }
  if (closure.err < 0) {
    return closure.err;
  }
  return writer_finish_section(w);
}

int writer_finish_public_section(writer *w) {
  if (w->block_writer == NULL) {
    return 0;
  }

  byte typ = block_writer_type(w->block_writer);
  int err = writer_finish_section(w);
  if (err < 0) {
    return err;
  }
  if (typ == BLOCK_TYPE_REF && !w->opts.skip_index_objects) {
    int err = writer_dump_object_index(w);
    if (err < 0) {
      return err;
    }
  }

  w->block_writer = NULL;
  return 0;
}

int writer_close(writer *w) {
  writer_finish_public_section(w);

  byte footer[68];
  byte *p = footer;
  writer_write_header(w, footer);
  p += 24;
  put_u64(p, w->stats.ref_stats.index_offset);
  p += 8;
  put_u64(p, (w->stats.obj_stats.offset) << 5 | w->stats.object_id_len);
  p += 8;
  put_u64(p, w->stats.obj_stats.index_offset);
  p += 8;
  put_u64(p, 0);
  p += 8;
  put_u64(p, 0);
  p += 8;

  // XXX compute CRC-32.
  put_u32(p, 0);
  p += 4;
  w->pending_padding = 0;

  slice out = {
      .buf = footer,
      .len = sizeof(footer),
  };
  int n = padded_write(w, out, 0);
  if (n < 0) {
    return n;
  }

  assert(n == sizeof(footer));

  // free up memory.
  block_writer_clear(&w->block_writer_data);
  writer_clear_index(w);
  free(slice_yield(&w->last_key));
  return 0;
}

void writer_clear_index(writer *w) {
  for (int i = 0; i < w->index_len; i++) {
    free(slice_yield(&w->index[i].last_key));
  }

  free(w->index);
  w->index = NULL;
  w->index_len = 0;
  w->index_cap = 0;
}

const int debug = 1;

int writer_flush_block(writer *w) {
  if (w->block_writer == NULL) {
    return 0;
  }
  if (w->block_writer->entries == 0) {
    return 0;
  }

  byte typ = block_writer_type(w->block_writer);

  block_stats *bstats = writer_block_stats(w, typ);
  if (bstats->blocks == 0) {
    bstats->offset = w->next;
  }

  int raw_bytes = block_writer_finish(w->block_writer);
  if (raw_bytes < 0) {
    return raw_bytes;
  }

  int padding = w->opts.block_size - raw_bytes;
  if (w->opts.unpadded || typ == BLOCK_TYPE_LOG) {
    padding = 0;
  }

  bstats->entries += w->block_writer->entries;
  bstats->restarts += w->block_writer->restart_len;
  bstats->blocks++;
  w->stats.blocks++;

  if (debug) {
    fprintf(stderr, "block %c off %ld sz %d (%d)\n", typ, w->next, raw_bytes,
            get_u24(w->block + w->block_writer->header_off + 1));
  }

  slice out = {
      .buf = w->block,
      .len = raw_bytes,
  };
  int n = padded_write(w, out, padding);
  if (n < 0) {
    return n;
  }

  if (w->index_cap == w->index_len) {
    w->index_cap = 2 * w->index_cap + 1;
    w->index = realloc(w->index, sizeof(index_record) * w->index_cap);
  }

  index_record ir = {
      .offset = w->next,
  };
  slice_copy(&ir.last_key, w->block_writer->last_key);
  w->index[w->index_len] = ir;
  w->index_len++;
  w->next += n;
  block_writer_reset(&w->block_writer_data);
  w->block_writer = NULL;
  return 0;
}

stats *writer_stats(writer *w) { return &w->stats; }
