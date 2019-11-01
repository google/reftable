#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "record.h"
#include "block.h"
#include "api.h"
#include "writer.h"
#include "tree.h"

typedef struct {
  bool present;
  uint64 offset;
  uint64 index_offset;
} reader_offsets;

typedef struct {
  block_source_ops *block_ops;
  void *  block_source_arg ;
  
  uint32 block_size;
  uint64 min_update_index;
  uint64 max_update_index;
  int object_id_len;

  reader_offsets ref_offsets;
  reader_offsets obj_offsets;
  reader_offsets log_offsets;
} reader;

int reader_get_block(reader * r, byte **dest, uint64 off, sz uint32) {
  if (off >= r->size) {
    return NULL;
  }

  if (off + sz > r->size) {
    sz = r.size - off;
  }

  return block_ops->read_block(r->block_source_arg, dest, off, sz);
}

int new_reader(reader **dest, block_source_ops* ops, void *block_source_arg) {
  reader *r = calloc(sizeof(reader),1);
  r->size = ops->size(block_source_arg) - FOOTER_SIZE;
  r->block_source_arg = block_source_arg;
  r->block_source_ops = ops;

  byte *footer = NULL;
  int n = reader_get_block(r, &footer, r->size, FOOTER_SIZE);
  if (n != FOOTER_SIZE) {
    return IO_ERROR;
  }

  byte *f = footer;
  if (memcmp(f, "REFT", 4)) {
    return FORMAT_ERROR;
  }
  f += 4;
  byte version  = *f++;
  if (version != 1) {
    return FORMAT_ERROR;
  }
  r->block_size = get_u24(f);

  f += 3;
  r->min_update_index = get_u64(f);
  f+=8;
  r->max_update_index = get_u64(f);
  f+=8;

  uint64 ref_index_off = get_u64(f);
  f += 8;
  uint64 obj_off = get_u64(f);
  f += 8;

  r->object_id_len = obj_off & ((1<<5)-1);
  obj_off >>= 5;
  
  uint64 obj_index_off = get_u64(f);
  f += 8;
  uint64 log_off = get_u64(f);
  f += 8;
  uint64 log_index_off = get_u64(f);
  f += 8;

  byte *header;
  int n = reader_get_block(r, &header, 0, HEADER_SIZE+1);
  if (n != HEADER_SIZE) {
    return IO_ERROR;
  }

  firstBlockTyp = header[HEADER_SIZE];
  r->ref_offsets.present = (firstBlockTyp == BLOCK_TYPE_REF);
  r->ref_offsets.offset = 0;
  r->ref_offsets.index_offset = ref_index_off;

  r->log_offsets.present = (firstBlockTyp == BLOCK_TYPE_LOG || log_off > 0);
  r->log_offsets.offset = log_off;
  r->log_offsets.index_offset = log_index_off;

  r->obj_offsets.present = obj_off > 0;
  r->obj_offsets.offset = obj_off;
  r->obj_offsets.index_offset = obj_index_off;

  ops->return_block(block_source_arg, footer);
  ops->return_block(block_source_arg, header);
  *dest = r;
  return 0; 
}
