// Copyright 2019 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "record.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "api.h"
#include "constants.h"

int is_block_type(byte typ) {
  switch (typ) {
    case BLOCK_TYPE_REF:
    case BLOCK_TYPE_LOG:
    case BLOCK_TYPE_OBJ:
    case BLOCK_TYPE_INDEX:
      return true;
  }
  return false;
}

int get_var_int(uint64_t *dest, struct slice in) {
  if (in.len == 0) {
    return -1;
  }

  int ptr = 0;
  uint64_t val = in.buf[ptr] & 0x7f;

  while (in.buf[ptr] & 0x80) {
    ptr++;
    if (ptr > in.len) {
      return -1;
    }
    val = (val + 1) << 7 | (uint64_t)(in.buf[ptr] & 0x7f);
  }

  *dest = val;
  return ptr + 1;
}

int put_var_int(struct slice dest, uint64_t val) {
  byte buf[10];

  int i = 9;
  buf[i] = (byte)(val & 0x7f);
  i--;
  while (true) {
    val >>= 7;
    if (!val) {
      break;
    }
    val--;
    buf[i] = 0x80 | (byte)(val & 0x7f);
    i--;
  }

  int n = sizeof(buf) - i - 1;
  if (dest.len < n) {
    return -1;
  }
  memcpy(dest.buf, &buf[i + 1], n);
  return n;
}

int common_prefix_size(struct slice a, struct slice b) {
  int p = 0;
  while (p < a.len && p < b.len) {
    if (a.buf[p] != b.buf[p]) {
      break;
    }
    p++;
  }

  return p;
}

int decode_string(struct slice *dest, struct slice in) {
  int start_len = in.len;
  uint64_t tsize = 0;
  int n = get_var_int(&tsize, in);
  if (n <= 0) {
    return -1;
  }
  in.buf += n;
  in.len -= n;
  if (in.len < tsize) {
    return -1;
  }

  slice_resize(dest, tsize + 1);
  dest->buf[tsize] = 0;
  memcpy(dest->buf, in.buf, tsize);
  in.buf += tsize;
  in.len -= tsize;

  return start_len - in.len;
}

int encode_key(bool *restart, struct slice dest, struct slice prev_key,
               struct slice key, byte extra) {
  struct slice start = dest;
  int prefix_len = common_prefix_size(prev_key, key);
  *restart = (prefix_len == 0);

  int n = put_var_int(dest, (uint64_t)prefix_len);
  if (n < 0) {
    return -1;
  }
  dest.buf += n;
  dest.len -= n;

  uint64_t suffix_len = key.len - prefix_len;
  n = put_var_int(dest, suffix_len << 3 | (uint64_t)extra);
  if (n < 0) {
    return -1;
  }
  dest.buf += n;
  dest.len -= n;

  if (dest.len < suffix_len) {
    return -1;
  }
  memcpy(dest.buf, key.buf + prefix_len, suffix_len);
  dest.buf += suffix_len;
  dest.len -= suffix_len;

  return start.len - dest.len;
}

byte ref_record_type() { return BLOCK_TYPE_REF; }

void ref_record_key(const void *r, struct slice *dest) {
  const struct ref_record *rec = (const struct ref_record *)r;
  slice_set_string(dest, rec->ref_name);
}

void ref_record_copy_from(void *rec, const void *src_rec) {
  struct ref_record *ref = (struct ref_record *)rec;
  struct ref_record *src = (struct ref_record *)src_rec;

  memset(ref, 0, sizeof(struct ref_record));
  if (src->ref_name != NULL) {
    ref->ref_name = strdup(src->ref_name);
  }

  if (src->target != NULL) {
    ref->target = strdup(src->target);
  }

  if (src->target_value != NULL) {
    ref->target_value = malloc(HASH_SIZE);
    memcpy(ref->target_value, src->target_value, HASH_SIZE);
  }

  if (src->value != NULL) {
    ref->value = malloc(HASH_SIZE);
    memcpy(ref->value, src->value, HASH_SIZE);
  }
  ref->update_index = src->update_index;
}

char hexdigit(int c) {
  if (c <= 9) {
    return '0' + c;
  }
  return 'a' + (c - 10);
}

void hex_format(char *dest, byte *src) {
  if (src != NULL) {
    for (int i = 0; i < HASH_SIZE; i++) {
      dest[2 * i] = hexdigit(src[i] >> 4);
      dest[2 * i + 1] = hexdigit(src[i] & 0xf);
    }
    dest[2 * HASH_SIZE] = 0;
  }
}

void ref_record_print(struct ref_record *ref) {
  char hex[41] = {};

  printf("ref{%s(%ld) ", ref->ref_name, ref->update_index);
  if (ref->value != NULL) {
    hex_format(hex, ref->value);
    printf("%s", hex);
  }
  if (ref->target_value != NULL) {
    hex_format(hex, ref->target_value);
    printf(" (T %s)", hex);
  }
  if (ref->target != NULL) {
    printf("=> %s", ref->target);
  }
  printf("}\n");
}

void ref_record_clear_void(void *rec) {
  ref_record_clear((struct ref_record *)rec);
}

void ref_record_clear(struct ref_record *ref) {
  free(ref->ref_name);
  free(ref->target);
  free(ref->target_value);
  free(ref->value);
  memset(ref, 0, sizeof(struct ref_record));
}

byte ref_record_val_type(const void *rec) {
  const struct ref_record *r = (const struct ref_record *)rec;
  if (r->value != NULL) {
    if (r->target_value != NULL) {
      return 2;
    } else {
      return 1;
    }
  } else if (r->target != NULL) {
    return 3;
  }
  return 0;
}

int ref_record_encode(const void *rec, struct slice s) {
  const struct ref_record *r = (const struct ref_record *)rec;
  struct slice start = s;
  int n = put_var_int(s, r->update_index);
  if (n < 0) {
    return -1;
  }
  s.buf += n;
  s.len -= n;

  if (r->value != NULL) {
    if (s.len < HASH_SIZE) {
      return -1;
    }
    memcpy(s.buf, r->value, HASH_SIZE);
    s.buf += HASH_SIZE;
    s.len -= HASH_SIZE;
  }

  if (r->target_value != NULL) {
    if (s.len < HASH_SIZE) {
      return -1;
    }
    memcpy(s.buf, r->target_value, HASH_SIZE);
    s.buf += HASH_SIZE;
    s.len -= HASH_SIZE;
  }

  if (r->target != NULL) {
    int l = strlen(r->target);
    n = put_var_int(s, l);
    if (n < 0) {
      return -1;
    }
    s.buf += n;
    s.len -= n;
    if (s.len < l) {
      return -1;
    }
    memcpy(s.buf, r->target, l);
    s.buf += l;
    s.len -= l;
  }

  return start.len - s.len;
}

int ref_record_decode(void *rec, struct slice key, byte val_type,
                      struct slice in) {
  struct ref_record *r = (struct ref_record *)rec;

  struct slice start = in;
  int n = get_var_int(&r->update_index, in);
  if (n < 0) {
    return n;
  }

  in.buf += n;
  in.len -= n;

  r->ref_name = realloc(r->ref_name, key.len + 1);
  memcpy(r->ref_name, key.buf, key.len);
  r->ref_name[key.len] = 0;
  switch (val_type) {
    case 1:
    case 2:
      if (in.len < HASH_SIZE) {
        return -1;
      }

      if (r->value == NULL) {
        r->value = malloc(HASH_SIZE);
      }
      memcpy(r->value, in.buf, HASH_SIZE);
      in.buf += HASH_SIZE;
      in.len -= HASH_SIZE;
      if (val_type == 1) {
        break;
      }
      if (r->target_value == NULL) {
        r->target_value = malloc(HASH_SIZE);
      }
      memcpy(r->target_value, in.buf, HASH_SIZE);
      in.buf += HASH_SIZE;
      in.len -= HASH_SIZE;
      break;
    case 3: {
      struct slice dest = {};
      int n = decode_string(&dest, in);
      if (n < 0) {
        return -1;
      }
      in.buf += n;
      in.len -= n;

      if (r->target != NULL) {
        free(r->target);
      }
      r->target = slice_to_string(dest);
      free(slice_yield(&dest));
    } break;

    case 0:
      break;
    default:
      abort();
      break;
  }

  return start.len - in.len;
}

int decode_key(struct slice *key, byte *extra, struct slice last_key,
               struct slice in) {
  int start_len = in.len;
  uint64_t prefix_len = 0;
  int n = get_var_int(&prefix_len, in);
  if (n < 0) {
    return -1;
  }
  in.buf += n;
  in.len -= n;

  if (prefix_len > last_key.len) {
    return -1;
  }

  uint64_t suffix_len = 0;
  n = get_var_int(&suffix_len, in);
  if (n <= 0) {
    return -1;
  }
  in.buf += n;
  in.len -= n;

  *extra = (byte)(suffix_len & 0x7);
  suffix_len >>= 3;

  if (in.len < suffix_len) {
    return -1;
  }

  slice_resize(key, suffix_len + prefix_len);
  memcpy(key->buf, last_key.buf, prefix_len);

  memcpy(key->buf + prefix_len, in.buf, suffix_len);
  in.buf += suffix_len;
  in.len -= suffix_len;

  return start_len - in.len;
}

struct record_ops ref_record_ops = {
    .key = &ref_record_key,
    .type = &ref_record_type,
    .copy_from = &ref_record_copy_from,
    .val_type = &ref_record_val_type,
    .encode = &ref_record_encode,
    .decode = &ref_record_decode,
    .clear = &ref_record_clear_void,
};

byte obj_record_type() { return BLOCK_TYPE_OBJ; }

void obj_record_key(const void *r, struct slice *dest) {
  const struct obj_record *rec = (const struct obj_record *)r;
  slice_resize(dest, rec->hash_prefix_len);
  memcpy(dest->buf, rec->hash_prefix, rec->hash_prefix_len);
}

void obj_record_copy_from(void *rec, const void *src_rec) {
  struct obj_record *ref = (struct obj_record *)rec;
  const struct obj_record *src = (const struct obj_record *)src_rec;

  *ref = *src;
  ref->hash_prefix = malloc(ref->hash_prefix_len);
  memcpy(ref->hash_prefix, src->hash_prefix, ref->hash_prefix_len);

  int olen = ref->offset_len * sizeof(uint64_t);
  ref->offsets = malloc(olen);
  memcpy(ref->offsets, src->offsets, olen);
}

void obj_record_clear(void *rec) {
  struct obj_record *ref = (struct obj_record *)rec;
  free(ref->hash_prefix);
  free(ref->offsets);
  memset(ref, 0, sizeof(struct obj_record));
}

byte obj_record_val_type(const void *rec) {
  struct obj_record *r = (struct obj_record *)rec;
  if (r->offset_len > 0 && r->offset_len < 8) {
    return r->offset_len;
  }
  return 0;
}

int obj_record_encode(const void *rec, struct slice s) {
  struct obj_record *r = (struct obj_record *)rec;
  struct slice start = s;
  if (r->offset_len == 0 || r->offset_len >= 8) {
    int n = put_var_int(s, r->offset_len);
    if (n < 0) {
      return -1;
    }
    s.buf += n;
    s.len -= n;
  }
  if (r->offset_len == 0) {
    return start.len - s.len;
  }
  int n = put_var_int(s, r->offsets[0]);
  if (n < 0) {
    return -1;
  }
  s.buf += n;
  s.len -= n;

  uint64_t last = r->offsets[0];
  for (int i = 1; i < r->offset_len; i++) {
    int n = put_var_int(s, r->offsets[i] - last);
    if (n < 0) {
      return -1;
    }
    s.buf += n;
    s.len -= n;
    last = r->offsets[i];
  }

  return start.len - s.len;
}

int obj_record_decode(void *rec, struct slice key, byte val_type,
                      struct slice in) {
  struct slice start = in;
  struct obj_record *r = (struct obj_record *)rec;

  r->hash_prefix = malloc(key.len);
  memcpy(r->hash_prefix, key.buf, key.len);
  r->hash_prefix_len = key.len;

  uint64_t count = val_type;
  if (val_type == 0) {
    int n = get_var_int(&count, in);
    if (n < 0) {
      return n;
    }

    in.buf += n;
    in.len -= n;
  }

  r->offsets = NULL;
  r->offset_len = 0;
  if (count == 0) {
    return start.len - in.len;
  }

  r->offsets = malloc(count * sizeof(uint64_t));
  r->offset_len = count;

  int n = get_var_int(&r->offsets[0], in);
  if (n < 0) {
    return n;
  }

  in.buf += n;
  in.len -= n;

  uint64_t last = r->offsets[0];

  int j = 1;
  while (j < count) {
    uint64_t delta = 0;
    int n = get_var_int(&delta, in);
    if (n < 0) {
      return n;
    }

    in.buf += n;
    in.len -= n;

    last = r->offsets[j] = (delta + last);
    j++;
  }
  return start.len - in.len;
}

struct record_ops obj_record_ops = {
    .key = &obj_record_key,
    .type = &obj_record_type,
    .copy_from = &obj_record_copy_from,
    .val_type = &obj_record_val_type,
    .encode = &obj_record_encode,
    .decode = &obj_record_decode,
    .clear = &obj_record_clear,
};

struct record new_record(byte typ) {
  struct record rec;
  switch (typ) {
    case BLOCK_TYPE_REF: {
      struct ref_record *r = calloc(1, sizeof(struct ref_record));
      record_from_ref(&rec, r);
      return rec;
    }

    case BLOCK_TYPE_OBJ: {
      struct obj_record *r = calloc(1, sizeof(struct obj_record));
      record_from_obj(&rec, r);
      return rec;
    }

    case BLOCK_TYPE_INDEX: {
      struct index_record *r = calloc(1, sizeof(struct index_record));
      record_from_index(&rec, r);
      return rec;
    }
  }
  abort();
  return rec;
}

byte index_record_type() { return BLOCK_TYPE_INDEX; }

void index_record_key(const void *r, struct slice *dest) {
  struct index_record *rec = (struct index_record *)r;
  slice_copy(dest, rec->last_key);
}

void index_record_copy_from(void *rec, const void *src_rec) {
  struct index_record *dst = (struct index_record *)rec;
  struct index_record *src = (struct index_record *)src_rec;

  slice_copy(&dst->last_key, src->last_key);
  dst->offset = src->offset;
}

void index_record_clear(void *rec) {
  struct index_record *idx = (struct index_record *)rec;
  free(slice_yield(&idx->last_key));
}

byte index_record_val_type(const void *rec) { return 0; }

int index_record_encode(const void *rec, struct slice out) {
  const struct index_record *r = (const struct index_record *)rec;
  struct slice start = out;

  int n = put_var_int(out, r->offset);
  if (n < 0) {
    return n;
  }

  out.buf += n;
  out.len -= n;

  return start.len - out.len;
}

int index_record_decode(void *rec, struct slice key, byte val_type,
                        struct slice in) {
  struct slice start = in;
  struct index_record *r = (struct index_record *)rec;

  slice_copy(&r->last_key, key);

  int n = get_var_int(&r->offset, in);
  if (n < 0) {
    return n;
  }

  in.buf += n;
  in.len -= n;
  return start.len - in.len;
}

struct record_ops index_record_ops = {
    .key = &index_record_key,
    .type = &index_record_type,
    .copy_from = &index_record_copy_from,
    .val_type = &index_record_val_type,
    .encode = &index_record_encode,
    .decode = &index_record_decode,
    .clear = &index_record_clear,
};

void record_key(struct record rec, struct slice *dest) {
  rec.ops->key(rec.data, dest);
}

byte record_type(struct record rec) { return rec.ops->type(); }

int record_encode(struct record rec, struct slice dest) {
  return rec.ops->encode(rec.data, dest);
}

void record_copy_from(struct record rec, struct record src) {
  assert(src.ops->type() == rec.ops->type());

  rec.ops->copy_from(rec.data, src.data);
}

byte record_val_type(struct record rec) { return rec.ops->val_type(rec.data); }

int record_decode(struct record rec, struct slice key, byte extra,
                  struct slice src) {
  return rec.ops->decode(rec.data, key, extra, src);
}

void record_clear(struct record rec) { return rec.ops->clear(rec.data); }

void record_from_ref(struct record *rec, struct ref_record *ref_rec) {
  rec->data = ref_rec;
  rec->ops = &ref_record_ops;
}

void record_from_obj(struct record *rec, struct obj_record *obj_rec) {
  rec->data = obj_rec;
  rec->ops = &obj_record_ops;
}

void record_from_index(struct record *rec, struct index_record *index_rec) {
  rec->data = index_rec;
  rec->ops = &index_record_ops;
}

void *record_yield(struct record *rec) {
  void *p = rec->data;
  rec->data = NULL;
  return p;
}

struct ref_record *record_as_ref(struct record rec) {
  assert(record_type(rec) == BLOCK_TYPE_REF);
  return (struct ref_record *)rec.data;
}

bool hash_equal(byte *a, byte *b) {
  if (a != NULL && b != NULL) {
    return 0 == memcmp(a, b, HASH_SIZE);
  }

  return a == b;
}

bool str_equal(char *a, char *b) {
  if (a != NULL && b != NULL) {
    return 0 == strcmp(a, b);
  }

  return a == b;
}

bool ref_record_equal(struct ref_record *a, struct ref_record *b) {
  return 0 == strcmp(a->ref_name, b->ref_name) &&
         a->update_index == b->update_index && hash_equal(a->value, b->value) &&
         hash_equal(a->target_value, b->target_value) &&
         str_equal(a->target, b->target);
}

bool ref_record_is_deletion(const struct ref_record *ref) {
  return ref->value == NULL && ref->target == NULL && ref->target_value == NULL;
}
