/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "reftable-malloc.h"

#include "basics.h"
#include "system.h"

static void *(*reftable_malloc_ptr)(size_t sz) = &malloc;
static void *(*reftable_realloc_ptr)(void *, size_t) = &realloc;
static void (*reftable_free_ptr)(void *) = &free;

void *reftable_malloc(size_t sz)
{
	return (*reftable_malloc_ptr)(sz);
}

void *reftable_realloc(void *p, size_t sz)
{
	return (*reftable_realloc_ptr)(p, sz);
}

void reftable_free(void *p)
{
	reftable_free_ptr(p);
}

void *reftable_calloc(size_t sz)
{
	void *p = reftable_malloc(sz);
	memset(p, 0, sz);
	return p;
}

void reftable_set_alloc(void *(*malloc)(size_t),
			void *(*realloc)(void *, size_t), void (*free)(void *))
{
	reftable_malloc_ptr = malloc;
	reftable_realloc_ptr = realloc;
	reftable_free_ptr = free;
}

int hash_size(uint32_t id)
{
	switch (id) {
	case 0:
	case SHA1_ID:
		return SHA1_SIZE;
	case SHA256_ID:
		return SHA256_SIZE;
	}
	abort();
}
