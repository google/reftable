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


#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "api.h"
#include "slice.h"

void slice_set_string(slice *s, const char *str) {
  if (str == NULL) {
    s->len = 0;
    return;
  }

  int l = strlen(str);
  l++; // \0
  slice_resize(s, l);
  memcpy(s->buf, str, l);
  s->len = l - 1;
}

void slice_resize(slice *s, int l) {
  if (s->cap < l) {
    int c = s->cap * 2;
    if (c < l) {
      c = l;
    }
    s->cap = c;
    s->buf = realloc(s->buf, s->cap);
  }
  s->len = l;
}

byte *slice_yield(slice *s) {
  byte *p = s->buf;
  s->buf = NULL;
  s->cap = 0;
  s->len = 0;
  return p;
}

void slice_copy(slice *dest, slice src) {
  slice_resize(dest, src.len);
  memcpy(dest->buf, src.buf, src.len);
}

char *slice_to_string(slice in) {
  slice s = {};
  slice_resize(&s, in.len + 1);
  s.buf[in.len] = 0;
  memcpy(s.buf, in.buf, in.len);
  return (char *)slice_yield(&s);
}

bool slice_equal(slice a, slice b) {
  if (a.len != b.len) {
    return 0;
  }
  return memcmp(a.buf, b.buf, a.len) == 0;
}

int slice_compare(slice a, slice b) {
  int min = b.len;
  if (a.len < b.len) {
    min = a.len;
  }
  int res = memcmp(a.buf, b.buf, min);
  if (res != 0) {
    return res;
  }
  if (a.len < b.len) {
    return -1;
  } else if (a.len > b.len) {
    return 1;
  } else {
    return 0;
  }
}

int slice_write(slice *b, byte *data, int sz) {
  if (b->len + sz > b->cap) {
    int newcap = 2 * b->cap + 1;
    if (newcap < b->len + sz) {
      newcap = (b->len + sz);
    }
    b->buf = realloc(b->buf, newcap);
    b->cap = newcap;
  }

  memcpy(b->buf + b->len, data, sz);
  b->len += sz;
  return sz;
}

int slice_write_void(void *b, byte *data, int sz) {
  return slice_write((slice *)b, data, sz);
}

uint64 slice_size(void *b) { return ((slice *)b)->len; }

void slice_return_block(void *b, block *dest) {
  memset(dest->data, 0xff, dest->len);
  free(dest->data);
}

void slice_close(void *b) {}

int slice_read_block(void *v, block *dest, uint64 off, uint32 size) {
  slice *b = (slice *)v;
  assert(off + size <= b->len);
  dest->data = calloc(size,1);
  memcpy(dest->data,  b->buf + off, size);
  dest->len = size;
  block_source_from_slice(&dest->source, b);
  return size;
}

block_source_ops slice_ops = {
    .size = &slice_size,
    .read_block = &slice_read_block,
    .return_block = &slice_return_block,
    .close = &slice_close,
};

void block_source_from_slice(block_source *bs, slice *buf) {
  bs->ops = &slice_ops;
  bs->arg = buf;
}
