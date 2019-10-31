#include "basics.h"

void put_u24(byte *out, uint32 i) {
  out[0] = (byte)((i >> 16) & 0xff);
  out[1] = (byte)((i >> 8) & 0xff);
  out[2] = (byte)((i) & 0xff);
}

uint32 get_u24(byte *in) {
  return (uint32)(in[0])<<16 | (uint32)(in[1])<<8 | (uint32)(in[2]);
}

void put_u32(byte *out, uint32 i) {
  out[0] = (byte)((i >> 24) & 0xff);
  out[1] = (byte)((i >> 16) & 0xff);
  out[2] = (byte)((i >> 8) & 0xff);
  out[3] = (byte)((i) & 0xff);
}

uint32 get_u32(byte *in) {
  return (uint32)(in[0])<<24 | (uint32)(in[1])<<16 | (uint32)(in[2])<<8 | (uint32)(in[3]);
}

void put_u64(byte *out, uint64 v) {
  for (int i = sizeof(uint64); i--; ) {
    out[i] = (byte) (v & 0xff);
    v >>= 8;
  }
}

uint64 get_u64(byte *out) {
  uint64 v = 0;
  for (int i = 0; i < sizeof(uint64); i++) {
    v = (v<<8)|(byte) (out[i] & 0xff);
  }
  return v;
}


void put_u16(byte *out, uint16 i) {
  out[0] = (byte)((i >> 8) & 0xff);
  out[1] = (byte)((i) & 0xff);
}

uint16 get_u16(byte *in) {
  return (uint32)(in[0])<<8 | (uint32)(in[1]);
}


/*
  find smallest index i in [0, sz) at which f(i) is true, assuming
  that f is ascending. Return sz if f(i) is false for all indices.
*/
int binsearch(int sz, int (*f)(int k, void *args), void*args) {
  int lo = 0;
  int hi = sz;

  /* invariant: (hi == sz) || f(hi) == true
     (lo == 0 && f(0) == true) || fi(lo) == false
   */
  while (hi - lo > 1) {
    int mid = lo + (hi - lo)/2 ;
    
    int val = f(mid, args);
    if (val) {
      hi = mid;
    } else {
      lo = mid;
    }
  }

  if (lo == 0) {
    if (f(0, args)) {
      return 0;
    } else {
      return 1;
    }
  }

  return hi;
}
