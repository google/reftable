#ifndef BYTES_H
#define BYTES_H

#include "basics.h"
#include "api.h"

typedef struct {
  byte *buf;
  int end, cap;
} buffer;

int buffer_write(void *b, byte *data, int sz);

void block_source_from_buffer(block_source *bs, buffer *buf);

#endif
