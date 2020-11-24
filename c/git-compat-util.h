/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#ifndef GIT_COMPAT_UTIL_H
#define GIT_COMPAT_UTIL_H

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <zlib.h>

/* functions that git-core provides, for standalone compilation */

uint64_t get_be64(void *in);
void put_be64(void *out, uint64_t i);

void put_be32(void *out, uint32_t i);
uint32_t get_be32(uint8_t *in);

uint16_t get_be16(uint8_t *in);

#define ARRAY_SIZE(a) sizeof((a)) / sizeof((a)[0])
#define FREE_AND_NULL(x)          \
	do {                      \
		reftable_free(x); \
		(x) = NULL;       \
	} while (0)
#define QSORT(arr, n, cmp) qsort(arr, n, sizeof(arr[0]), cmp)
#define SWAP(a, b)                              \
	{                                       \
		char tmp[sizeof(a)];            \
		assert(sizeof(a) == sizeof(b)); \
		memcpy(&tmp[0], &a, sizeof(a)); \
		memcpy(&a, &b, sizeof(a));      \
		memcpy(&b, &tmp[0], sizeof(a)); \
	}

char *xstrdup(const char *s);

void sleep_millisec(int millisecs);

#endif
