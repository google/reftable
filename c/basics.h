#ifndef BASICS_H
#define BASICS_H

#include <stdint.h>

#define true 1
#define false 0
#define ARRAYSIZE(a) sizeof(a)/sizeof(a[0])

typedef uint64_t uint64;
typedef uint32_t uint32;
typedef uint16_t uint16;
typedef uint8_t byte;
typedef byte bool;
typedef int error;

void put_u24(byte *out, uint32 i) ;
uint32 get_u24(byte *in) ;
void put_u16(byte *out, uint16 i) ;
uint16 get_u16(byte *in) ;
int binsearch(int sz, int (*f)(int k, void *args), void*args);

#endif
