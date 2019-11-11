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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "api.h"
#include "iter.h"
#include "block.h"
#include "reader.h"
#include "record.h"
#include "tree.h"

uint64 block_source_size(block_source source) {
  return source.ops->size(source.arg);
}

int block_source_read_block(block_source source, block *dest, uint64 off,
                            uint32 size) {
  return source.ops->read_block(source.arg, dest, off, size);
}

void block_source_return_block(block_source source, block *blockp) {
  source.ops->return_block(source.arg, blockp);
}

void block_source_close(block_source source) { source.ops->close(source.arg); }

reader_offsets *reader_offsets_for(reader *r, byte typ) {
  switch (typ) {
  case BLOCK_TYPE_REF:
    return &r->ref_offsets;
  case BLOCK_TYPE_LOG:
    return &r->log_offsets;
  case BLOCK_TYPE_OBJ:
    return &r->obj_offsets;
  }
  abort();
}

int reader_get_block(reader *r, block *dest, uint64 off, uint32 sz) {
  if (off >= r->size) {
    return 0;
  }

  if (off + sz > r->size) {
    sz = r->size - off;
  }

  return block_source_read_block(r->source, dest, off, sz);
}

void reader_return_block(reader *r, block *p) {
  block_source_return_block(r->source, p);
}

int init_reader(reader *r, block_source source) {
  memset(r, 0, sizeof(reader));
  r->size = block_source_size(source) - FOOTER_SIZE;
  r->source = source;

  block footer = {};
  block header = {};
  
  int err = block_source_read_block(source, &footer, r->size, FOOTER_SIZE);
  if (err != FOOTER_SIZE) {
    err = IO_ERROR;
    goto exit;
  }

  err = reader_get_block(r, &header, 0, HEADER_SIZE + 1);
  if (err != HEADER_SIZE + 1) {
    err = IO_ERROR;
    goto exit;
  }

  byte *f = footer.data;
  if (memcmp(f, "REFT", 4)) {
    err = FORMAT_ERROR;
    goto exit;
  }
  f += 4;
  byte version = *f++;
  if (version != 1) {
    err = FORMAT_ERROR;
    goto exit;
  }
  r->block_size = get_u24(f);

  f += 3;
  r->min_update_index = get_u64(f);
  f += 8;
  r->max_update_index = get_u64(f);
  f += 8;

  uint64 ref_index_off = get_u64(f);
  f += 8;
  uint64 obj_off = get_u64(f);
  f += 8;

  r->object_id_len = obj_off & ((1 << 5) - 1);
  obj_off >>= 5;

  uint64 obj_index_off = get_u64(f);
  f += 8;
  uint64 log_off = get_u64(f);
  f += 8;
  uint64 log_index_off = get_u64(f);
  f += 8;

  byte first_block_typ = header.data[HEADER_SIZE];
  r->ref_offsets.present = (first_block_typ == BLOCK_TYPE_REF);
  r->ref_offsets.offset = 0;
  r->ref_offsets.index_offset = ref_index_off;

  r->log_offsets.present = (first_block_typ == BLOCK_TYPE_LOG || log_off > 0);
  r->log_offsets.offset = log_off;
  r->log_offsets.index_offset = log_index_off;

  r->obj_offsets.present = obj_off > 0;
  r->obj_offsets.offset = obj_off;
  r->obj_offsets.index_offset = obj_index_off;

  err = 0;
exit:
  block_source_return_block(r->source, &footer);
  block_source_return_block(r->source, &header);
  return err;
}

typedef struct {
  reader *r;
  byte typ;
  uint64 block_off;
  block_iter bi;
  bool finished;
} table_iter;

void table_iter_copy_from(table_iter *dest, table_iter *src) {
  dest->r = src->r;
  dest->typ = src->typ;
  dest->block_off = src->block_off;
  dest->finished = src->finished;
  block_iter_copy_from(&dest->bi, &src->bi);
}

int table_iter_next_in_block(table_iter *ti, record rec) {
  int res = block_iter_next(&ti->bi, rec);
  if (res == 0 && record_type(rec) == BLOCK_TYPE_REF) {
    ((ref_record *)rec.data)->update_index += ti->r->min_update_index;
  }

  return res;
}

void table_iter_block_done(table_iter *ti) {
  if (ti->bi.br == NULL) {
    return;
  }
  reader_return_block(ti->r, &ti->bi.br->block);
  free(ti->bi.br);
  ti->bi.br = NULL;

  ti->bi.last_key.len = 0;
  ti->bi.next_off = 0;
}

int32 extract_block_size(byte *data, byte *typ, uint64 off) {
  if (off == 0) {
    data += 24;
  }

  *typ = data[0];
  int32 result = 0;
  if (is_block_type(*typ)) {
    result = get_u24(data + 1);
  }
  return result;
}

int reader_init_block_reader(reader *r, block_reader *br, uint64 next_off,
                             byte want_typ) { 
 if (next_off >= r->size) {
    return 1;
  }

  int32 guess_block_size = r->block_size;
  if (guess_block_size == 0 ) {
    guess_block_size = DEFAULT_BLOCK_SIZE;
  }

  block block = {};
  int32 read_size = reader_get_block(r, &block, next_off, guess_block_size);
  if (read_size < 0) {
    return read_size;
  }

  byte block_typ = 0;
  int32 block_size = extract_block_size(block.data, &block_typ, next_off);
  if (block_size < 0) {
    return block_size;
  }

  if (want_typ != BLOCK_TYPE_ANY && block_typ != want_typ) {
    return 1;
  }

  if (block_size > guess_block_size) {
    reader_return_block(r, &block);
    int err = reader_get_block(r, &block, next_off, block_size);
    if (err < 0) {
      return err;
    }
  }
  
  uint32 header_off = 0;
  if (next_off == 0) {
    header_off = HEADER_SIZE;
  }

  return block_reader_init(br, &block, header_off, r->block_size);
}

int table_iter_next_block(table_iter *dest, table_iter *src) {
  uint64 next_block_off = src->block_off + src->bi.br->full_block_size;
  dest->r = src->r;
  dest->typ = src->typ;
  dest->block_off = next_block_off;

  block_reader br = {};
  int err = reader_init_block_reader(src->r, &br, next_block_off, src->typ);
  if (err > 0) {
    dest->finished = true;
    return 1;
  }
  if (err != 0) {
    return err;
  }

  block_reader *brp = malloc(sizeof(block_reader));
  *brp = br;

  dest->finished = false;
  block_reader_start(brp, &dest->bi);
  return 0;
}

int table_iter_next(table_iter *ti, record rec) {
  if (ti->finished) {
    return 1;
  }

  int err = table_iter_next_in_block(ti, rec);
  if (err <= 0) {
    return err;
  }

  table_iter next = {};
  err = table_iter_next_block(&next, ti);
  if (err != 0) {
    ti->finished = true;
  }
  table_iter_block_done(ti);
  if (err != 0) {
    return err;
  }
  table_iter_copy_from(ti, &next);
  block_iter_close(&next.bi);
  return block_iter_next(&ti->bi, rec);
}

int table_iter_next_void(void *ti, record rec) {
  return table_iter_next((table_iter *)ti, rec);
}

void table_iter_close(void *p) {
  table_iter *ti = (table_iter *)p;
  table_iter_block_done(ti);
  block_iter_close(&ti->bi);
}

struct _iterator_ops table_iter_ops = {
    .next = &table_iter_next_void,
    .close = &table_iter_close,
};

void iterator_from_table_iter(iterator *it, table_iter *ti) {
  it->iter_arg = ti;
  it->ops = &table_iter_ops;
}

int reader_table_iter_at(reader *r, table_iter *ti, uint64 off, byte typ) {
  block_reader br = {};
  int err = reader_init_block_reader(r, &br, off, typ);
  if (err != 0) {
    return err;
  }

  block_reader *brp = malloc(sizeof(block_reader));
  *brp = br;
  ti->r = r;
  ti->typ = block_reader_type(brp);
  ti->block_off = off;
  block_reader_start(brp, &ti->bi);
  return 0;
}

int reader_start(reader *r, table_iter *ti, byte typ, bool index) {
  reader_offsets *offs = reader_offsets_for(r, typ);
  uint64 off = offs->offset;
  if (index) {
    off = offs->index_offset;
    if (off == 0) {
      return 1;
    }
    typ = BLOCK_TYPE_INDEX;
  }

  return reader_table_iter_at(r, ti, off, typ);
}

int reader_seek_linear(reader *r, table_iter *ti, record want) {
  record rec = new_record(record_type(want));
  slice want_key = {};
  slice got_key = {};
  record_key(want, &want_key);
  int err = -1;

  table_iter next = {};
  while (true) {
    err = table_iter_next_block(&next, ti);
    if (err < 0) {
      goto exit;
    }

    if (err > 0) {
      break;
    }

    err = block_reader_first_key(next.bi.br, &got_key);
    if (err < 0) {
      goto exit;
    }
    int cmp = slice_compare(got_key, want_key);
    if (cmp > 0) {
      table_iter_block_done(&next);
      break;
    }

    table_iter_block_done(ti);
    table_iter_copy_from(ti, &next);
  }

  err = block_iter_seek(&ti->bi, want_key);
  if (err < 0) {
    goto exit;
  }
  err = 0;

exit:
  block_iter_close(&next.bi);
  record_clear(rec);
  free(record_yield(&rec));
  free(slice_yield(&want_key));
  free(slice_yield(&got_key));
  return err;
}

int reader_seek_indexed(reader *r, iterator *it, record rec) {
  index_record want_index  = {};
  record_key(rec, &want_index.last_key);
  record want_index_rec  = {};
  record_from_index(&want_index_rec, &want_index);
  index_record index_result = {};
  record index_result_rec = {};
  record_from_index(&index_result_rec, &index_result);

  table_iter index_iter = {};
  int err = reader_start(r, &index_iter, record_type(rec), true);
  if (err < 0) {
    goto exit;
  }

  err = reader_seek_linear(r, &index_iter, want_index_rec);
  table_iter next = {};
  while (true) {
    err = table_iter_next(&index_iter, index_result_rec);
    table_iter_block_done(&index_iter);
    if (err != 0) {
      goto exit;
    }

    err = reader_table_iter_at(r, &next, index_result.offset, 0);
    if (err != 0) {
      goto exit;
    }

    err = block_iter_seek(&next.bi, want_index.last_key);
    if (err < 0){
      goto exit;
    }

    if (next.typ == record_type(rec)) {
      err = 0;
      break;
    }

    if (next.typ != BLOCK_TYPE_INDEX) {
      err = FORMAT_ERROR;
      break;
    }

    table_iter_copy_from(&index_iter, &next);
  }

  if (err == 0) {
    table_iter *malloced = calloc(sizeof(table_iter), 1);
    table_iter_copy_from(malloced, &next);
    iterator_from_table_iter(it, malloced);
  }
 exit:
  block_iter_close(&next.bi);
  table_iter_close(&index_iter);
  record_clear(want_index_rec);
  record_clear(index_result_rec);
  return err;
}

int reader_seek_internal(reader *r, iterator *it, record rec) {
  reader_offsets *offs = reader_offsets_for(r, record_type(rec));
  uint64 idx = offs->index_offset;
  if (idx > 0) {
    return reader_seek_indexed(r, it, rec);
  }

  table_iter ti = {};
  int err = reader_start(r, &ti, record_type(rec), false);
  if (err < 0) {
    return err;
  }
  err = reader_seek_linear(r, &ti, rec);
  if (err < 0) {
    return err;
  }

  table_iter *p = malloc(sizeof(table_iter));
  *p = ti;
  iterator_from_table_iter(it, p);

  return 0;
}

int reader_seek(reader *r, iterator *it, record rec) {
  byte typ = record_type(rec);

  reader_offsets *offs = reader_offsets_for(r, typ);
  if (!offs->present) {
    iterator_set_empty(it);
    return 0;
  }

  return reader_seek_internal(r, it, rec);
}

int reader_seek_ref(reader *r, iterator *it, char *name) {
  ref_record ref = {
		    .ref_name = name,
  };
  record rec ={};
  record_from_ref(&rec, &ref);
  return reader_seek(r, it, rec);
}

void reader_close(reader *r) { block_source_close(r->source); }

int new_reader(reader **p, block_source src) {
  reader *rd = calloc(sizeof(reader), 1);
  int err = init_reader(rd, src);
  if (err == 0) {
    *p= rd;
  } else {
    free(rd);
  }
  return err;
}


void reader_free(reader *r) {
  reader_close(r);
  free(r);
}

int reader_refs_for_indexed(reader* r, iterator *it, byte *oid) {
  obj_record want = {
		     .hash_prefix = oid,
		     .hash_prefix_len = r->object_id_len,
  };
  record want_rec  = {};
  record_from_obj(&want_rec, &want);

  iterator oit = {};
  int err = reader_seek(r, &oit, want_rec);
  if (err != 0) {
    return err;
  }

  obj_record got = {};
  record got_rec = {};
  record_from_obj(&got_rec, &got);
  err = iterator_next(oit, got_rec);
  iterator_destroy(&oit);
  if(err < 0) {
    return err;
  }

  if (err > 0 || memcmp(want.hash_prefix, got.hash_prefix, r->object_id_len)) {
    iterator_set_empty(it);
    return 0;
  }

  indexed_table_ref_iter* itr = NULL;
  err = new_indexed_table_ref_iter(&itr, r, oid, got.offsets, got.offset_len);
  if (err  < 0) {
    record_clear(got_rec);
    return err;
  }
  got.offsets = NULL;
  record_clear(got_rec);
  
  iterator_from_indexed_table_ref_iter(it, itr);
  return 0;
}

int reader_refs_for(reader* r, iterator *it, byte *oid) {
  if (r->obj_offsets.present) {
    return reader_refs_for_indexed(r, it, oid);
  }

  table_iter * ti = calloc(sizeof(table_iter), 1);
  int err = reader_start(r, ti, BLOCK_TYPE_REF, false);
  if (err < 0) { return err ; }

  filtering_ref_iterator* filter = calloc(sizeof(filtering_ref_iterator), 1);
  filter->oid = oid;
  filter->r = r;
  filter->double_check = false;
  iterator_from_table_iter(&filter->it, ti);

  iterator_from_filtering_ref_iterator(it, filter);
  return 0;
}
