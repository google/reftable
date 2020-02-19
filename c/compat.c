/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "system.h"

void put_be32(uint8_t *out, uint32_t i)
{
	out[0] = (uint8_t)((i >> 24) & 0xff);
	out[1] = (uint8_t)((i >> 16) & 0xff);
	out[2] = (uint8_t)((i >> 8) & 0xff);
	out[3] = (uint8_t)((i)&0xff);
}

uint32_t get_be32(uint8_t *in)
{
	return (uint32_t)(in[0]) << 24 | (uint32_t)(in[1]) << 16 |
	       (uint32_t)(in[2]) << 8 | (uint32_t)(in[3]);
}

void put_be64(uint8_t *out, uint64_t v)
{
	int i = sizeof(uint64_t);
	while (i--) {
		out[i] = (uint8_t)(v & 0xff);
		v >>= 8;
	}
}

uint64_t get_be64(uint8_t *out)
{
	uint64_t v = 0;
	int i = 0;
	for (i = 0; i < sizeof(uint64_t); i++) {
		v = (v << 8) | (uint8_t)(out[i] & 0xff);
	}
	return v;
}

uint16_t get_be16(uint8_t *in)
{
	return (uint32_t)(in[0]) << 8 | (uint32_t)(in[1]);
}

char *xstrdup(const char *s)
{
	int l = strlen(s);
	char *dest = (char *)malloc(l + 1);
	strncpy(dest, s, l + 1);
	return dest;
}
