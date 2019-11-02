#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "bytes.h"

int buffer_write(void *v, byte *data, int sz) {
  buffer *b = (buffer*)v;
  if  (b->end + sz > b->cap) {
    int newcap =  2*b->cap + 1;
    if (newcap < b->end + sz) {
      newcap = (b->end + sz);
    }
    b->buf = realloc(b->buf, newcap);
    b->cap  = newcap;
  }

  memcpy(b->buf + b->end, data, sz);
  b->end += sz;
  return sz;
}

uint64 buffer_size(void *b) {
  
  return ((buffer*) b)->end;
}

void buffer_return_block(void *b, byte *dest) {
  free(dest);
}

void buffer_close(void *b) {

}

int buffer_read_block(void *v, byte **dest, uint64 off, uint32 size) {
  buffer *b  = (buffer*)v;
  *dest = malloc(size);

  assert(off + size < b->end);
  memcpy(dest, b->buf + off, size);
  return size;
}

block_source_ops buffer_ops = {
			       .size = &buffer_size,
			       .read_block = &buffer_read_block,
			       .return_block = &buffer_return_block,
			       .close = &buffer_close,
};

void block_source_from_buffer(block_source *bs, buffer *buf) {
  bs->ops = &buffer_ops;
  bs->arg = buf;
}
