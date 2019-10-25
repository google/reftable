#include <string.h>

#include "api.h"

int get_var_int(uint64 *dest, slice in) {
  if (in.len == 0) {
    return -1;
  }

  int ptr = 0;
  uint64 val = in.buf[ptr] & 0x7f;

  while ( in.buf[ptr] & 0x80 ) {
    ptr++;
    if (ptr > in.len) {
      return -1;
    }
    val = (val + 1) << 7 | (uint64)(in.buf[ptr]&0x7f);
  }

  *dest = val;
  return ptr+1;
}

int put_var_int(slice dest, uint64 val) {
  byte buf[10];

  int i = 9;
  buf[i] = (byte)(val & 0x7f);
  i--;
  while (true) {
    val >>= 7;
    if (!val) {
      break;
    }
    val--;
    buf[i] = 0x80 | (byte)(val&0x7f);
    i--;
  }

  int n = sizeof(buf) - i - 1;
  if (dest.len < n) { return -1; }
  memcpy(dest.buf, &buf[i+1], n);
  return n;
}
