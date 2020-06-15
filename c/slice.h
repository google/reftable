/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#ifndef SLICE_H
#define SLICE_H

#include "basics.h"
#include "reftable.h"

/*
  Provides a bounds-checked, growable byte ranges. To use, initialize as "slice
  x = SLICE_INIT;"
 */
struct slice {
	int len;
	int cap;
	byte *buf;

	/* Used to enforce initialization with SLICE_INIT */
	byte canary;
};
#define SLICE_CANARY 0x42
#define SLICE_INIT                       \
	{                                \
		0, 0, NULL, SLICE_CANARY \
	}
extern struct slice reftable_empty_slice;

void slice_addstr(struct slice *dest, const char *src);

/* Deallocate and clear slice */
void slice_release(struct slice *slice);

/* Set slice to 0 length, but retain buffer. */
void slice_reset(struct slice *slice);

/* Initializes a slice. Accepts a slice with random garbage. */
void slice_init(struct slice *slice);

/* Ensure that `buf` is \0 terminated. */
const char *slice_as_string(struct slice *src);

/* Return `buf`, clearing out `s` */
char *slice_detach(struct slice *s);

/* Set length of the slace to `l`, but don't reallocated. */
void slice_setlen(struct slice *s, size_t l);

/* Ensure `l` bytes beyond current length are available */
void slice_grow(struct slice *s, size_t l);

/* Signed comparison */
int slice_cmp(const struct slice *a, const struct slice *b);

/* Append `data` to the `dest` slice.  */
int slice_add(struct slice *dest, const byte *data, size_t sz);

/* Append `add` to `dest. */
void slice_addbuf(struct slice *dest, struct slice *add);

/* Like slice_add, but suitable for passing to reftable_new_writer
 */
int slice_add_void(void *b, byte *data, size_t sz);

/* Find the longest shared prefix size of `a` and `b` */
int common_prefix_size(struct slice *a, struct slice *b);

struct reftable_block_source;

/* Create an in-memory block source for reading reftables */
void block_source_from_slice(struct reftable_block_source *bs,
			     struct slice *buf);

struct reftable_block_source malloc_block_source(void);

/* Advance `buf` by `n`, and decrease length. A copy of the slice
   should be kept for deallocating the slice. */
void slice_consume(struct slice *s, int n);

#endif
