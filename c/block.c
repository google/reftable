#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "api.h"
#include "block.h"
#include "record.h"

int block_writer_register_restart(block_writer *w, int n, bool restart,
                                  slice key);

void block_writer_init(block_writer *bw, byte typ, byte *buf, uint32 block_size,
		       uint32 header_off) {
  bw->buf = buf;
  bw->block_size = block_size;
  bw->header_off = header_off;
  bw->buf[header_off] = typ;
  bw->next = header_off + 4;
  bw->restart_interval = 16;
}

byte block_writer_type(block_writer *bw) { return bw->buf[bw->header_off]; }

int block_writer_add(block_writer *w, record rec) {
  slice last = w->last_key;
  if (w->entries % w->restart_interval == 0) {
    last.len = 0;
  }

  slice out = {
      .buf = w->buf + w->next,
      .len = w->block_size - w->next,
  };

  slice start = out;

  bool restart = false;
  slice key = {};
  record_key(rec, &key);
  int n = encode_key(&restart, out, last, key, record_val_type(rec));
  if (n < 0) {
    goto err;
  }
  out.buf += n;
  out.len -= n;

  n = record_encode(rec, out);
  if (n < 0) {
    goto err;
  }

  out.buf += n;
  out.len -= n;

  if (block_writer_register_restart(w, start.len - out.len, restart, key) < 0) {
    goto err;
  }

  free(slice_yield(&key));
  return 0;

err:
  free(slice_yield(&key));
  return -1;
}

int block_writer_register_restart(block_writer *w, int n, bool restart,
                                  slice key) {
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
      w->restarts = realloc(w->restarts, sizeof(uint32) * w->restart_cap);
    }

    w->restarts[w->restart_len++] = w->next;
  }

  w->next += n;
  slice_copy(&w->last_key, key);
  w->entries++;
  return 0;
}

int block_writer_finish(block_writer *w) {
  for (int i = 0; i < w->restart_len; i++) {
    put_u24(w->buf + w->next, w->restarts[i]);
    w->next += 3;
  }

  put_u16(w->buf + w->next, w->restart_len);
  w->next += 2;
  put_u24(w->buf + 1 + w->header_off, w->next);

  // TODO - do log compression here.

  return w->next;
}

byte block_reader_type(block_reader *r) { return r->block[r->header_off]; }

int block_reader_init(block_reader *br, byte *block, uint32 header_off,
		      uint32 table_block_size) {
  uint32 full_block_size = table_block_size;
  byte typ = block[header_off];

  if (!is_block_type(typ)) {
    return FORMAT_ERROR;
  }

  uint32 sz = get_u24(block + header_off + 1);

  if (typ == BLOCK_TYPE_LOG) {
    abort();

    /* TODO: decompress log block, record how many bytes consumed. */
  } else if (full_block_size == 0) {
    full_block_size = sz;
  }

  uint16 restart_count = get_u16(block + sz - 2);
  uint32 restart_start = sz - 2 - 3 * restart_count;

  byte *restart_bytes = block + restart_start;

  br->block = block;
  br->block_len = restart_start;
  br->full_block_size = full_block_size;
  br->header_off = header_off;
  br->restart_count = restart_count;
  br->restart_bytes = restart_bytes;

  return 0;
}

uint32 block_reader_restart_offset(block_reader *br, int i) {
  return get_u24(br->restart_bytes + 3 * i);
}

void block_reader_start(block_reader *br, block_iter *it) {
  it->br = br;
  slice_resize(&it->last_key, 0);
  it->next_off = br->header_off + 4;
}

typedef struct {
  slice key;
  block_reader *r;
  int error;
} restart_find_args;

int key_less(int idx, void *args) {
  restart_find_args *a = (restart_find_args *)args;
  uint32 off = block_reader_restart_offset(a->r, idx);
  slice in = {
      .buf = a->r->block,
      .len = a->r->block_len,
  };
  in.buf += off;
  in.len -= off;

  // XXX could avoid alloc here.
  slice rkey = {};
  slice last_key = {};
  byte extra;
  int n = decode_key(&rkey, &extra, last_key, in);
  if (n < 0) {
    a->error = 1;
    return -1;
  }

  return slice_compare(a->key, rkey);
}

// return < 0 for error, 0 for OK, > 0 for EOF.
int block_iter_next(block_iter *it, record rec) {
  if (it->next_off >= it->br->block_len) {
    return 1;
  }

  slice in = {
      .buf = it->br->block + it->next_off,
      .len = it->br->block_len - it->next_off,
  };
  slice start = in;
  slice key = {};
  byte extra;
  int n = decode_key(&key, &extra, it->last_key, in);
  if (n < 0) {
    return -1;
  }

  in.buf += n;
  in.len -= n;
  n = record_decode(rec, key, extra, in);
  if (n < 0) {
    return -1;
  }
  in.buf += n;
  in.len -= n;

  slice_copy(&it->last_key, key);
  it->next_off += start.len - in.len;
  return 0;
}

int block_iter_seek(block_iter *it, slice want) {
  return block_reader_seek(it->br, it, want);
}

int block_reader_seek(block_reader *br, block_iter *it, slice want) {
  restart_find_args args = {
      .key = want,
      .r = br,
  };

  int i = binsearch(br->restart_count, &key_less, &args);
  if (args.error) {
    return -1;
  }

  if (i > 0) {
    i--;
    it->next_off = block_reader_restart_offset(br, i);
  } else {
    it->next_off = br->header_off + 4;
  }

  record rec = new_record(block_reader_type(br));
  slice key = {};
  int result = 0;
  while (true) {
    block_iter next = *it;
    int ok = block_iter_next(&next, rec);
    if (ok < 0) {
      result = -1;
      goto exit;
    }

    record_key(rec, &key);
    if (ok > 0 || slice_compare(key, want) >= 0) {
      result = 0;
      goto exit;
    }

    *it = next;
  }

exit:
  record_clear(rec);
  free(record_yield(&rec));
  return result;
}

void block_writer_reset(block_writer *bw) {
  bw->restart_len = 0;
  bw->last_key.len = 0;
}

void block_writer_free(block_writer *bw) {
  free(bw->restarts);
  free(slice_yield(&bw->last_key));
  // the block is not owned.
}
