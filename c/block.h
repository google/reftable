#ifndef BLOCK_H
#define BLOCK_H

#include "basics.h"
#include "api.h"

typedef struct _block_writer  block_writer;

block_writer *new_block_writer(byte typ, byte *buf, uint32 block_size, uint32 header_off);
int block_writer_add(block_writer *w, record *rec);
  int block_writer_finish(block_writer *w);

typedef struct _block_reader block_reader;

typedef struct {
  block_reader *br;

  slice last_key;
  uint32 next_off;
} block_iter;


block_reader* new_block_reader(byte *block,  uint32 header_off , uint32 table_block_size);
int block_reader_start(block_reader* br, block_iter* it);
int block_iter_next(block_iter *it, record* rec);

#endif
