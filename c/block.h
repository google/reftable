#ifndef BLOCK_H
#define BLOCK_H

#include "api.h"
#include "basics.h"

typedef struct _block_reader block_reader;
typedef struct _block_writer block_writer;
typedef struct _block_iter block_iter;

struct _block_writer {
  byte *buf;
  uint32 block_size;
  uint32 header_off;
  int restart_interval;

  uint32 next;
  uint32 *restarts;
  uint32 restart_len;
  uint32 restart_cap;
  slice last_key;
  int entries;
};

void block_writer_init(block_writer *bw, byte typ, byte *buf, uint32 block_size,
                       uint32 header_off);
byte block_writer_type(block_writer *bw);
int block_writer_add(block_writer *w, record rec);
int block_writer_finish(block_writer *w);
void block_writer_reset(block_writer *bw);
void block_writer_clear(block_writer *bw);

struct _block_reader {
  uint32 header_off;
  byte *block;
  uint32 block_len;
  byte *restart_bytes;
  uint32 full_block_size;
  uint16 restart_count;
};

int block_reader_init(block_reader *br, byte *block, uint32 header_off,
                      uint32 table_block_size);
void block_reader_start(block_reader *br, block_iter *it);
int block_reader_seek(block_reader *br, block_iter *it, slice want);
byte block_reader_type(block_reader *r);
int block_reader_first_key(block_reader *br, slice *key);

struct _block_iter {
  block_reader *br;

  slice last_key;
  uint32 next_off;
};

void block_iter_copy_from(block_iter *dest, block_iter *src);
int block_iter_next(block_iter *it, record rec);
int block_iter_seek(block_iter *it, slice want);
void block_iter_close(block_iter *it);

#endif
