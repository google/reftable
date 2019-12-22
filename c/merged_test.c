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

#include <string.h>

#include "api.h"
#include "basics.h"
#include "block.h"
#include "pq.h"
#include "reader.h"
#include "record.h"
#include "test_framework.h"
#include "merged.h"

void test_pq(void) {
  char *names[54] = {};
  int N = ARRAYSIZE(names)-1;

  for (int i = 0; i < N; i++) {
    char name[100];
    sprintf(name, "%02d", i);
    names[i] = strdup(name);
  }

  struct merged_iter_pqueue pq = {};

  int i = 1;
  do {
    struct record rec = new_record(BLOCK_TYPE_REF);
    record_as_ref(rec)->ref_name = names[i];

    struct pq_entry e = {
        .rec = rec,
    };
    merged_iter_pqueue_add(&pq, e);
    merged_iter_pqueue_check(pq);
    i = (i * 7) % N;
  } while (i != 1);

  const char *last = NULL;
  while (!merged_iter_pqueue_is_empty(pq)) {
    struct pq_entry e = merged_iter_pqueue_remove(&pq);
    merged_iter_pqueue_check(pq);
    struct ref_record *ref = record_as_ref(e.rec);

    if (last != NULL) {
      assert(strcmp(last, ref->ref_name) < 0);
    }
    last = ref->ref_name;
    ref->ref_name = NULL;
    free(ref);
  }

  for (int i = 0; i < N; i++) {
    free(names[i]);
  }

  merged_iter_pqueue_clear(&pq);
}

void write_test_table(struct slice *buf, struct ref_record refs[], int n) {
  int min = 0xffffffff;
  int max = 0;
  for (int i = 0; i < n; i++) {
    uint64_t ui = refs[i].update_index;
    if (ui > max) {
      max = ui;
    }
    if (ui < min) {
      min = ui;
    }
  }

  struct write_options opts = {
      .block_size = 256,
  };

  struct writer *w = new_writer(&slice_write_void, buf, &opts);
  writer_set_limits(w, min, max);

  for (int i = 0; i < n; i++) {
    uint64_t before = refs[i].update_index;
    int n = writer_add_ref(w, &refs[i]);
    assert(n == 0);
    assert(before == refs[i].update_index);
  }

  int err = writer_close(w);
  assert(err == 0);

  writer_free(w);
  w = NULL;
}

void test_merged(void) {
  byte hash1[HASH_SIZE];
  byte hash2[HASH_SIZE];

  set_test_hash(hash1, 1);
  set_test_hash(hash2, 2);
  struct ref_record r1[] = {{
                                .ref_name = "a",
                                .update_index = 1,
                                .value = hash1,
                            },
                            {
                                .ref_name = "b",
                                .update_index = 1,
                                .value = hash1,
                            },
                            {
                                .ref_name = "c",
                                .update_index = 1,
                                .value = hash1,
                            }};
  struct ref_record r2[] = {{
      .ref_name = "a",
      .update_index = 2,
  }};
  struct ref_record r3[] = {
      {
          .ref_name = "c",
          .update_index = 3,
          .value = hash2,
      },
      {
          .ref_name = "d",
          .update_index = 3,
          .value = hash1,
      },
  };

  struct ref_record *refs[] = {r1, r2, r3};
  int sizes[3] = {3, 1, 2};
  struct slice buf[3] = {};
  struct block_source source[3] = {};
  struct reader **rd = calloc(sizeof(struct reader*), 3);
  for (int i = 0; i < 3; i++) {
    write_test_table(&buf[i], refs[i], sizes[i]);
    block_source_from_slice(&source[i], &buf[i]);

    int err = new_reader(&rd[i], source[i], "name");
    assert(err == 0);
  }

  struct merged_table *mt = NULL;
  int err = new_merged_table(&mt, rd, 3);
  assert(err == 0);

  struct iterator it = {};
  err = merged_table_seek_ref(mt, &it, "a");
  assert(err == 0);

  struct ref_record *out = NULL;
  int len = 0;
  int cap = 0;
  while (len < 100) { // cap loops/recursion.
    struct ref_record ref = {};
    int err = iterator_next_ref(it, &ref);
    if (err > 0) {
      break;
    }
    ref_record_print(&ref);
    if (len == cap) {
      cap = 2 * cap + 1;
      out = realloc(out, sizeof(struct ref_record) * cap);
    }
    out[len++] = ref;
  }
  iterator_destroy(&it);
  
  struct ref_record want[] = {
      r2[0],
      r1[1],
      r3[0],
      r3[1],
  };
  assert(ARRAYSIZE(want) == len);
  for (int i = 0; i < len; i++) {
    assert(ref_record_equal(&want[i], &out[i]));
  }
  for (int i = 0; i < len; i++) {
    ref_record_clear(&out[i]);
  }
  free(out);
  
  for (int i = 0 ; i < ARRAYSIZE(buf); i++) {
    free(slice_yield(&buf[i]));
  }
  merged_table_close(mt);
  merged_table_free(mt);
}

int main() {
  add_test_case("test_pq", &test_pq);
  add_test_case("test_merged", &test_merged);
  test_main();
}
