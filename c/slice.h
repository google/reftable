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
  provides bounds-checked byte ranges.
  To use, initialize as "slice x = {0};"
 */
struct slice {
	int len;
	int cap;
	byte *buf;
};

void slice_set_string(struct slice *dest, const char *src);
void slice_addstr(struct slice *dest, const char *src);

/* Deallocate and clear slice */
void slice_release(struct slice *slice);

/* Return a malloced string for `src` */
char *slice_to_string(struct slice src);

/* Ensure that `buf` is \0 terminated. */
const char *slice_as_string(struct slice *src);

/* Compare slices */
bool slice_equal(struct slice a, struct slice b);

/* Return `buf`, clearing out `s` */
byte *slice_detach(struct slice *s);

/* Copy bytes */
void slice_copy(struct slice *dest, struct slice src);

/* Advance `buf` by `n`, and decrease length. A copy of the slice
   should be kept for deallocating the slice. */
void slice_consume(struct slice *s, int n);

/* Set length of the slice to `l` */
void slice_resize(struct slice *s, int l);

/* Signed comparison */
int slice_cmp(struct slice a, struct slice b);

/* Append `data` to the `dest` slice.  */
int slice_add(struct slice *dest, byte *data, size_t sz);

/* Append `add` to `dest. */
void slice_addbuf(struct slice *dest, struct slice add);

/* Like slice_add, but suitable for passing to reftable_new_writer
 */
int slice_add_void(void *b, byte *data, size_t sz);

/* Find the longest shared prefix size of `a` and `b` */
int common_prefix_size(struct slice a, struct slice b);

struct reftable_block_source;

/* Create an in-memory block source for reading reftables */
void block_source_from_slice(struct reftable_block_source *bs,
			     struct slice *buf);

struct reftable_block_source malloc_block_source(void);

#endif
