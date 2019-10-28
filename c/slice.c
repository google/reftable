
#include <string.h>
#include <stdlib.h>

#include "slice.h"

void slice_init_from_string(slice *s, const char *str) {
  int l = strlen(str);
  l++; // \0
  slice_resize(s, l);
  memcpy(s->buf, str, l);
  s->len = l-1;
}

void slice_resize(slice *s, int l) {
  if (s->cap < l) {
    int c = s->cap * 2;
    if (c  < l ) {
      c = l;
    }
    s->cap = c;
    s->buf = realloc(s->buf, s->cap);
  }
  s->len = l;
}

void slice_free(slice *s){
  free(s->buf);
  s->buf = NULL;
  s->cap = 0;
  s->len = 0;
}

byte *slice_yield(slice *s) {
  byte* p = s->buf;
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
  return (char*) slice_yield(&s);
}

bool slice_equal(slice a, slice b) {
  if (a.len != b.len) { return 0; }
  return memcmp(a.buf, b.buf, a.len) == 0;
}


int slice_compare(slice a, slice b) {
  int min = b.len;
  if (a.len < b.len) {
    min = a.len;
  } 
  int res = memcmp(a.buf, b.buf, min);
  if (res != 0) { return res; }
  if(a.len < b.len) {
    return -1;
  } else if (a.len > b.len) {
    return 1;
  } else {
    return 0;
  }
}
