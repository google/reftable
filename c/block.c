/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "block.h"

#include "system.h"

#include "constants.h"
#include "record.h"
#include "reftable.h"
#include "zlib.h"

int header_size(int version)
{
	switch (version) {
	case 1:
		return 24;
	case 2:
		return 28;
	}
	abort();
}

int footer_size(int version)
{
	switch (version) {
	case 1:
		return 68;
	case 2:
		return 72;
	}
	abort();
}

int block_writer_register_restart(struct block_writer *w, int n, bool restart,
				  struct slice key);

void block_writer_init(struct block_writer *bw, byte typ, byte *buf,
		       uint32_t block_size, uint32_t header_off, int hash_size)
{
	bw->buf = buf;
	bw->hash_size = hash_size;
	bw->block_size = block_size;
	bw->header_off = header_off;
	bw->buf[header_off] = typ;
	bw->next = header_off + 4;
	bw->restart_interval = 16;
	bw->entries = 0;
	bw->restart_len = 0;
	bw->last_key.len = 0;
}

byte block_writer_type(struct block_writer *bw)
{
	return bw->buf[bw->header_off];
}

/* adds the reftable_record to the block. Returns -1 if it does not fit, 0 on
   success */
int block_writer_add(struct block_writer *w, struct reftable_record *rec)
{
	struct slice empty = { 0 };
	struct slice last = w->entries % w->restart_interval == 0 ? empty :
								    w->last_key;
	struct slice out = {
		.buf = w->buf + w->next,
		.len = w->block_size - w->next,
	};

	struct slice start = out;

	bool restart = false;
	struct slice key = { 0 };
	int n = 0;

	reftable_record_key(rec, &key);
	n = reftable_encode_key(&restart, out, last, key,
				reftable_record_val_type(rec));
	if (n < 0) {
		goto err;
	}
	slice_consume(&out, n);

	n = reftable_record_encode(rec, out, w->hash_size);
	if (n < 0) {
		goto err;
	}
	slice_consume(&out, n);

	if (block_writer_register_restart(w, start.len - out.len, restart,
					  key) < 0) {
		goto err;
	}

	slice_release(&key);
	return 0;

err:
	slice_release(&key);
	return -1;
}

int block_writer_register_restart(struct block_writer *w, int n, bool restart,
				  struct slice key)
{
	int rlen = w->restart_len;
	if (rlen >= MAX_RESTARTS) {
		restart = false;
	}

	if (restart) {
		rlen++;
	}
	if (2 + 3 * rlen + n > w->block_size - w->next) {
		return -1;
	}
	if (restart) {
		if (w->restart_len == w->restart_cap) {
			w->restart_cap = w->restart_cap * 2 + 1;
			w->restarts = reftable_realloc(
				w->restarts, sizeof(uint32_t) * w->restart_cap);
		}

		w->restarts[w->restart_len++] = w->next;
	}

	w->next += n;
	slice_copy(&w->last_key, key);
	w->entries++;
	return 0;
}

int block_writer_finish(struct block_writer *w)
{
	int i = 0;
	for (i = 0; i < w->restart_len; i++) {
		put_be24(w->buf + w->next, w->restarts[i]);
		w->next += 3;
	}

	put_be16(w->buf + w->next, w->restart_len);
	w->next += 2;
	put_be24(w->buf + 1 + w->header_off, w->next);

	if (block_writer_type(w) == BLOCK_TYPE_LOG) {
		int block_header_skip = 4 + w->header_off;
		struct slice compressed = { 0 };
		int zresult = 0;
		uLongf src_len = w->next - block_header_skip;
		slice_resize(&compressed, src_len);

		while (1) {
			uLongf dest_len = compressed.len;

			zresult = compress2(compressed.buf, &dest_len,
					    w->buf + block_header_skip, src_len,
					    9);
			if (zresult == Z_BUF_ERROR) {
				slice_resize(&compressed, 2 * compressed.len);
				continue;
			}

			if (Z_OK != zresult) {
				slice_release(&compressed);
				return REFTABLE_ZLIB_ERROR;
			}

			memcpy(w->buf + block_header_skip, compressed.buf,
			       dest_len);
			w->next = dest_len + block_header_skip;
			slice_release(&compressed);
			break;
		}
	}
	return w->next;
}

byte block_reader_type(struct block_reader *r)
{
	return r->block.data[r->header_off];
}

int block_reader_init(struct block_reader *br, struct reftable_block *block,
		      uint32_t header_off, uint32_t table_block_size,
		      int hash_size)
{
	uint32_t full_block_size = table_block_size;
	byte typ = block->data[header_off];
	uint32_t sz = get_be24(block->data + header_off + 1);

	if (!reftable_is_block_type(typ)) {
		return REFTABLE_FORMAT_ERROR;
	}

	if (typ == BLOCK_TYPE_LOG) {
		struct slice uncompressed = { 0 };
		int block_header_skip = 4 + header_off;
		uLongf dst_len = sz - block_header_skip; /* total size of dest
							    buffer. */
		uLongf src_len = block->len - block_header_skip;

		/* Log blocks specify the *uncompressed* size in their header.
		 */
		slice_resize(&uncompressed, sz);

		/* Copy over the block header verbatim. It's not compressed. */
		memcpy(uncompressed.buf, block->data, block_header_skip);

		/* Uncompress */
		if (Z_OK != uncompress_return_consumed(
				    uncompressed.buf + block_header_skip,
				    &dst_len, block->data + block_header_skip,
				    &src_len)) {
			slice_release(&uncompressed);
			return REFTABLE_ZLIB_ERROR;
		}

		if (dst_len + block_header_skip != sz) {
			return REFTABLE_FORMAT_ERROR;
		}

		/* We're done with the input data. */
		reftable_block_done(block);
		block->data = uncompressed.buf;
		block->len = sz;
		block->source = malloc_block_source();
		full_block_size = src_len + block_header_skip;
	} else if (full_block_size == 0) {
		full_block_size = sz;
	} else if (sz < full_block_size && sz < block->len &&
		   block->data[sz] != 0) {
		/* If the block is smaller than the full block size, it is
		   padded (data followed by '\0') or the next block is
		   unaligned. */
		full_block_size = sz;
	}

	{
		uint16_t restart_count = get_be16(block->data + sz - 2);
		uint32_t restart_start = sz - 2 - 3 * restart_count;

		byte *restart_bytes = block->data + restart_start;

		/* transfer ownership. */
		br->block = *block;
		block->data = NULL;
		block->len = 0;

		br->hash_size = hash_size;
		br->block_len = restart_start;
		br->full_block_size = full_block_size;
		br->header_off = header_off;
		br->restart_count = restart_count;
		br->restart_bytes = restart_bytes;
	}

	return 0;
}

static uint32_t block_reader_restart_offset(struct block_reader *br, int i)
{
	return get_be24(br->restart_bytes + 3 * i);
}

void block_reader_start(struct block_reader *br, struct block_iter *it)
{
	it->br = br;
	slice_resize(&it->last_key, 0);
	it->next_off = br->header_off + 4;
}

struct restart_find_args {
	int error;
	struct slice key;
	struct block_reader *r;
};

static int restart_key_less(size_t idx, void *args)
{
	struct restart_find_args *a = (struct restart_find_args *)args;
	uint32_t off = block_reader_restart_offset(a->r, idx);
	struct slice in = {
		.buf = a->r->block.data + off,
		.len = a->r->block_len - off,
	};

	/* the restart key is verbatim in the block, so this could avoid the
	   alloc for decoding the key */
	struct slice rkey = { 0 };
	struct slice last_key = { 0 };
	byte unused_extra;
	int n = reftable_decode_key(&rkey, &unused_extra, last_key, in);
	if (n < 0) {
		a->error = 1;
		return -1;
	}

	{
		int result = slice_cmp(a->key, rkey);
		slice_release(&rkey);
		return result;
	}
}

void block_iter_copy_from(struct block_iter *dest, struct block_iter *src)
{
	dest->br = src->br;
	dest->next_off = src->next_off;
	slice_copy(&dest->last_key, src->last_key);
}

int block_iter_next(struct block_iter *it, struct reftable_record *rec)
{
	struct slice in = {
		.buf = it->br->block.data + it->next_off,
		.len = it->br->block_len - it->next_off,
	};
	struct slice start = in;
	struct slice key = { 0 };
	byte extra = 0;
	int n = 0;

	if (it->next_off >= it->br->block_len) {
		return 1;
	}

	n = reftable_decode_key(&key, &extra, it->last_key, in);
	if (n < 0) {
		return -1;
	}

	slice_consume(&in, n);
	n = reftable_record_decode(rec, key, extra, in, it->br->hash_size);
	if (n < 0) {
		return -1;
	}
	slice_consume(&in, n);

	slice_copy(&it->last_key, key);
	it->next_off += start.len - in.len;
	slice_release(&key);
	return 0;
}

int block_reader_first_key(struct block_reader *br, struct slice *key)
{
	struct slice empty = { 0 };
	int off = br->header_off + 4;
	struct slice in = {
		.buf = br->block.data + off,
		.len = br->block_len - off,
	};

	byte extra = 0;
	int n = reftable_decode_key(key, &extra, empty, in);
	if (n < 0) {
		return n;
	}
	return 0;
}

int block_iter_seek(struct block_iter *it, struct slice want)
{
	return block_reader_seek(it->br, it, want);
}

void block_iter_close(struct block_iter *it)
{
	slice_release(&it->last_key);
}

int block_reader_seek(struct block_reader *br, struct block_iter *it,
		      struct slice want)
{
	struct restart_find_args args = {
		.key = want,
		.r = br,
	};
	struct reftable_record rec = reftable_new_record(block_reader_type(br));
	struct slice key = { 0 };
	int err = 0;
	struct block_iter next = { 0 };

	int i = binsearch(br->restart_count, &restart_key_less, &args);
	if (args.error) {
		err = REFTABLE_FORMAT_ERROR;
		goto exit;
	}

	it->br = br;
	if (i > 0) {
		i--;
		it->next_off = block_reader_restart_offset(br, i);
	} else {
		it->next_off = br->header_off + 4;
	}

	/* We're looking for the last entry less/equal than the wanted key, so
	   we have to go one entry too far and then back up.
	*/
	while (true) {
		block_iter_copy_from(&next, it);
		err = block_iter_next(&next, &rec);
		if (err < 0) {
			goto exit;
		}

		reftable_record_key(&rec, &key);
		if (err > 0 || slice_cmp(key, want) >= 0) {
			err = 0;
			goto exit;
		}

		block_iter_copy_from(it, &next);
	}

exit:
	slice_release(&key);
	slice_release(&next.last_key);
	reftable_record_destroy(&rec);

	return err;
}

void block_writer_clear(struct block_writer *bw)
{
	FREE_AND_NULL(bw->restarts);
	slice_release(&bw->last_key);
	/* the block is not owned. */
}
