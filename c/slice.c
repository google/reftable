/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "slice.h"

#include "system.h"

#include "reftable.h"

struct slice reftable_empty_slice = SLICE_INIT;

void slice_init(struct slice *s)
{
	struct slice empty = SLICE_INIT;
	*s = empty;
}

void slice_grow(struct slice *s, size_t extra)
{
	size_t newcap = s->len + extra + 1;
	if (newcap > s->cap) {
		s->buf = reftable_realloc(s->buf, newcap);
		s->cap = newcap;
	}
}

static void slice_resize(struct slice *s, int l)
{
	int zl = l + 1; /* one byte for 0 termination. */
	assert(s->canary == SLICE_CANARY);
	if (s->cap < zl) {
		int c = s->cap * 2;
		if (c < zl) {
			c = zl;
		}
		s->cap = c;
		s->buf = reftable_realloc(s->buf, s->cap);
	}
	s->len = l;
	s->buf[l] = 0;
}

void slice_setlen(struct slice *s, size_t l)
{
	assert(s->cap >= l + 1);
	s->len = l;
	s->buf[l] = 0;
}

void slice_reset(struct slice *s)
{
	slice_resize(s, 0);
}

void slice_addstr(struct slice *d, const char *s)
{
	int l1 = d->len;
	int l2 = strlen(s);
	assert(d->canary == SLICE_CANARY);

	slice_resize(d, l2 + l1);
	memcpy(d->buf + l1, s, l2);
}

void slice_addbuf(struct slice *s, struct slice *a)
{
	int end = s->len;
	assert(s->canary == SLICE_CANARY);
	slice_resize(s, s->len + a->len);
	memcpy(s->buf + end, a->buf, a->len);
}

void slice_consume(struct slice *s, int n)
{
	assert(s->canary == SLICE_CANARY);
	s->buf += n;
	s->len -= n;
}

char *slice_detach(struct slice *s)
{
	char *p = NULL;
	slice_as_string(s);
	p = (char *)s->buf;
	s->buf = NULL;
	s->cap = 0;
	s->len = 0;
	return p;
}

void slice_release(struct slice *s)
{
	byte *ptr = s->buf;
	assert(s->canary == SLICE_CANARY);
	s->buf = NULL;
	s->cap = 0;
	s->len = 0;
	reftable_free(ptr);
}

/* return the underlying data as char*. len is left unchanged, but
   a \0 is added at the end. */
const char *slice_as_string(struct slice *s)
{
	return (const char *)s->buf;
}

int slice_cmp(const struct slice *a, const struct slice *b)
{
	int min = a->len < b->len ? a->len : b->len;
	int res = memcmp(a->buf, b->buf, min);
	assert(a->canary == SLICE_CANARY);
	assert(b->canary == SLICE_CANARY);
	if (res != 0)
		return res;
	if (a->len < b->len)
		return -1;
	else if (a->len > b->len)
		return 1;
	else
		return 0;
}

int slice_add(struct slice *b, const byte *data, size_t sz)
{
	assert(b->canary == SLICE_CANARY);
	slice_grow(b, sz);
	memcpy(b->buf + b->len, data, sz);
	b->len += sz;
	b->buf[b->len] = 0;
	return sz;
}

int slice_add_void(void *b, byte *data, size_t sz)
{
	return slice_add((struct slice *)b, data, sz);
}

static uint64_t slice_size(void *b)
{
	return ((struct slice *)b)->len;
}

static void slice_return_block(void *b, struct reftable_block *dest)
{
	memset(dest->data, 0xff, dest->len);
	reftable_free(dest->data);
}

static void slice_close(void *b)
{
}

static int slice_read_block(void *v, struct reftable_block *dest, uint64_t off,
			    uint32_t size)
{
	struct slice *b = (struct slice *)v;
	assert(off + size <= b->len);
	dest->data = reftable_calloc(size);
	memcpy(dest->data, b->buf + off, size);
	dest->len = size;
	return size;
}

struct reftable_block_source_vtable slice_vtable = {
	.size = &slice_size,
	.read_block = &slice_read_block,
	.return_block = &slice_return_block,
	.close = &slice_close,
};

void block_source_from_slice(struct reftable_block_source *bs,
			     struct slice *buf)
{
	assert(bs->ops == NULL);
	bs->ops = &slice_vtable;
	bs->arg = buf;
}

static void malloc_return_block(void *b, struct reftable_block *dest)
{
	memset(dest->data, 0xff, dest->len);
	reftable_free(dest->data);
}

struct reftable_block_source_vtable malloc_vtable = {
	.return_block = &malloc_return_block,
};

struct reftable_block_source malloc_block_source_instance = {
	.ops = &malloc_vtable,
};

struct reftable_block_source malloc_block_source(void)
{
	return malloc_block_source_instance;
}

int common_prefix_size(struct slice *a, struct slice *b)
{
	int p = 0;
	assert(a->canary == SLICE_CANARY);
	assert(b->canary == SLICE_CANARY);
	while (p < a->len && p < b->len) {
		if (a->buf[p] != b->buf[p]) {
			break;
		}
		p++;
	}

	return p;
}
