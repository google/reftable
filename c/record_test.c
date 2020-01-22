/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "record.h"

#include <string.h>

#include "basics.h"
#include "constants.h"
#include "reftable.h"
#include "test_framework.h"

void varint_roundtrip() {
  uint64_t inputs[] = {0,
                       1,
                       27,
                       127,
                       128,
                       257,
                       4096,
                       ((uint64_t)1 << 63),
                       ((uint64_t)1 << 63) + ((uint64_t)1 << 63) - 1};
  for (int i = 0; i < ARRAYSIZE(inputs); i++) {
    byte dest[10];

    struct slice out = {.buf = dest, .len = 10, .cap = 10};

    uint64_t in = inputs[i];
    int n = put_var_int(out, in);
    assert(n > 0);
    out.len = n;

    uint64_t got = 0;
    n = get_var_int(&got, out);
    assert(n > 0);

    assert(got == in);
  }
}

void test_common_prefix() {
  struct {
    const char *a, *b;
    int want;
  } cases[] = {
      {"abc", "ab", 2},
      {"", "abc", 0},
      {"abc", "abd", 2},
      {"abc", "pqr", 0},
  };

  for (int i = 0; i < ARRAYSIZE(cases); i++) {
    struct slice a = {};
    struct slice b = {};
    slice_set_string(&a, cases[i].a);
    slice_set_string(&b, cases[i].b);

    int got = common_prefix_size(a, b);
    assert(got == cases[i].want);

    free(slice_yield(&a));
    free(slice_yield(&b));
  }
}

void set_hash(byte *h, int j) {
  for (int i = 0; i < SHA1_SIZE; i++) {
    h[i] = (j >> i) & 0xff;
  }
}

void test_ref_record_roundtrip() {
  for (int i = 0; i <= 3; i++) {
    printf("subtest %d\n", i);
    struct ref_record in = {};
    switch (i) {
      case 0:
        break;
      case 1:
        in.value = malloc(SHA1_SIZE);
        set_hash(in.value, 1);
        break;
      case 2:
        in.value = malloc(SHA1_SIZE);
        set_hash(in.value, 1);
        in.target_value = malloc(SHA1_SIZE);
        set_hash(in.target_value, 2);
        break;
      case 3:
        in.target = strdup("target");
        break;
    }
    in.ref_name = strdup("refs/heads/master");

    struct record rec = {};
    record_from_ref(&rec, &in);
    assert(record_val_type(rec) == i);
    byte buf[1024];
    struct slice key = {};
    record_key(rec, &key);
    struct slice dest = {
        .buf = buf,
        .len = sizeof(buf),
    };
    int n = record_encode(rec, dest, SHA1_SIZE);
    assert(n > 0);

    struct ref_record out = {};
    struct record rec_out = {};
    record_from_ref(&rec_out, &out);
    int m = record_decode(rec_out, key, i, dest, SHA1_SIZE);
    assert(n == m);

    assert((out.value != NULL) == (in.value != NULL));
    assert((out.target_value != NULL) == (in.target_value != NULL));
    assert((out.target != NULL) == (in.target != NULL));
    free(slice_yield(&key));
    record_clear(rec_out);
    ref_record_clear(&in);
  }
}

void test_log_record_roundtrip() {
  struct log_record in = {
      .ref_name = strdup("refs/heads/master"),
      .old_hash = malloc(SHA1_SIZE),
      .new_hash = malloc(SHA1_SIZE),
      .name = strdup("han-wen"),
      .email = strdup("hanwen@google.com"),
      .message = strdup("test"),
      .update_index = 42,
      .time = 1577123507,
      .tz_offset = 100,
  };

  struct record rec = {};
  record_from_log(&rec, &in);

  struct slice key = {};
  record_key(rec, &key);

  byte buf[1024];
  struct slice dest = {
      .buf = buf,
      .len = sizeof(buf),
  };

  int n = record_encode(rec, dest, SHA1_SIZE);
  assert(n > 0);

  struct log_record out = {};
  struct record rec_out = {};
  record_from_log(&rec_out, &out);
  int valtype = record_val_type(rec);
  int m = record_decode(rec_out, key, valtype, dest, SHA1_SIZE);
  assert(n == m);

  assert(log_record_equal(&in, &out, SHA1_SIZE));
  log_record_clear(&in);
  free(slice_yield(&key));
  record_clear(rec_out);
}

void test_u24_roundtrip() {
  uint32_t in = 0x112233;
  byte dest[3];

  put_u24(dest, in);
  uint32_t out = get_u24(dest);
  assert(in == out);
}

void test_key_roundtrip() {
  struct slice dest = {}, last_key = {}, key = {}, roundtrip = {};

  slice_resize(&dest, 1024);
  slice_set_string(&last_key, "refs/heads/master");
  slice_set_string(&key, "refs/tags/bla");

  bool restart;
  byte extra = 6;
  int n = encode_key(&restart, dest, last_key, key, extra);
  assert(!restart);
  assert(n > 0);

  byte rt_extra;
  int m = decode_key(&roundtrip, &rt_extra, last_key, dest);
  assert(n == m);
  assert(slice_equal(key, roundtrip));
  assert(rt_extra == extra);

  free(slice_yield(&last_key));
  free(slice_yield(&key));
  free(slice_yield(&dest));
  free(slice_yield(&roundtrip));
}

void print_bytes(byte *p, int l) {
  for (int i = 0; i < l; i++) {
    byte c = *p;
    if (c < 32) {
      c = '.';
    }
    printf("%02x[%c] ", p[i], c);
  }
  printf("(%d)\n", l);
}

void test_obj_record_roundtrip() {
  byte testHash1[SHA1_SIZE] = {};
  set_hash(testHash1, 1);
  uint64_t till9[] = {1, 2, 3, 4, 500, 600, 700, 800, 9000};

  struct obj_record recs[3] = {{
                                   .hash_prefix = testHash1,
                                   .hash_prefix_len = 5,
                                   .offsets = till9,
                                   .offset_len = 3,
                               },
                               {
                                   .hash_prefix = testHash1,
                                   .hash_prefix_len = 5,
                                   .offsets = till9,
                                   .offset_len = 9,
                               },
                               {
                                   .hash_prefix = testHash1,
                                   .hash_prefix_len = 5,
                               }

  };
  for (int i = 0; i < ARRAYSIZE(recs); i++) {
    printf("subtest %d\n", i);
    struct obj_record in = recs[i];
    byte buf[1024];
    struct record rec = {};
    record_from_obj(&rec, &in);
    struct slice key = {};
    record_key(rec, &key);
    struct slice dest = {
        .buf = buf,
        .len = sizeof(buf),
    };
    int n = record_encode(rec, dest, SHA1_SIZE);
    assert(n > 0);
    byte extra = record_val_type(rec);
    struct obj_record out = {};
    struct record rec_out = {};
    record_from_obj(&rec_out, &out);
    int m = record_decode(rec_out, key, extra, dest, SHA1_SIZE);
    assert(n == m);

    assert(in.hash_prefix_len == out.hash_prefix_len);
    assert(in.offset_len == out.offset_len);

    assert(0 == memcmp(in.hash_prefix, out.hash_prefix, in.hash_prefix_len));
    assert(0 ==
           memcmp(in.offsets, out.offsets, sizeof(uint64_t) * in.offset_len));
    free(slice_yield(&key));
    record_clear(rec_out);
  }
}

void test_index_record_roundtrip() {
  struct index_record in = {.offset = 42};

  slice_set_string(&in.last_key, "refs/heads/master");

  struct slice key = {};
  struct record rec = {};
  record_from_index(&rec, &in);
  record_key(rec, &key);

  assert(0 == slice_compare(key, in.last_key));

  byte buf[1024];
  struct slice dest = {
      .buf = buf,
      .len = sizeof(buf),
  };
  int n = record_encode(rec, dest, SHA1_SIZE);
  assert(n > 0);

  byte extra = record_val_type(rec);
  struct index_record out = {};
  struct record out_rec;
  record_from_index(&out_rec, &out);
  int m = record_decode(out_rec, key, extra, dest, SHA1_SIZE);
  assert(m == n);

  assert(in.offset == out.offset);

  record_clear(out_rec);
  free(slice_yield(&key));
  free(slice_yield(&in.last_key));
}

int main() {
  add_test_case("test_log_record_roundtrip", &test_log_record_roundtrip);
  add_test_case("test_ref_record_roundtrip", &test_ref_record_roundtrip);
  add_test_case("varint_roundtrip", &varint_roundtrip);
  add_test_case("test_key_roundtrip", &test_key_roundtrip);
  add_test_case("test_common_prefix", &test_common_prefix);
  add_test_case("test_obj_record_roundtrip", &test_obj_record_roundtrip);
  add_test_case("test_index_record_roundtrip", &test_index_record_roundtrip);
  add_test_case("test_u24_roundtrip", &test_u24_roundtrip);
  test_main();
}
