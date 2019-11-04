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
#include "reader.h"
#include "record.h"
#include "test_framework.h"

void test_buffer(void) {
  slice buf = {};

  byte in[] = "hello";
  slice_write(&buf, in, sizeof(in));
  block_source source;
  block_source_from_slice(&source, &buf);
  assert(block_source_size(source) == 6);
  byte *out = NULL;
  int n = block_source_read_block(source, &out, 0, sizeof(in));
  assert(n == sizeof(in));
  assert(0 == memcmp(in, out, n));
  block_source_return_block(source, out);

  n = block_source_read_block(source, &out, 1, 2);
  assert(n == 2);
  assert(0 == memcmp(out, "el", 2));

  block_source_close(source);
  free(slice_yield(&buf));
}


void write_table(char ***names, slice *buf, int N, int block_size) {
  *names = calloc(sizeof(char*), N);
  
  write_options opts = {
      .block_size = 256,
  };

  writer *w = new_writer(&slice_write_void, buf, &opts);

  {
    ref_record ref = {};
    for (int i = 0; i < N; i++) {
      byte hash[20];
      memset(hash, i, sizeof(hash));

      char name[100];
      sprintf(name, "refs/heads/branch%02d", i);

      ref.ref_name = name;
      ref.value = hash;
      (*names)[i] = strdup(name);

      fflush(stdout);
      int n = writer_add_ref(w, &ref);
      assert(n == 0);
    }
  }
  int n = writer_close(w);
  assert(n == 0);

  stats * stats = writer_stats(w);
  for (int i = 0; i < stats->ref_stats.blocks; i++) {
    int off = i * opts.block_size;
    if (off == 0) {
      off = HEADER_SIZE;
    }
    assert(buf->buf[off] == 'r');
  }

  writer_free(w);
  w = NULL;
}


void test_table_read_write_sequential(void) {
  char **names;
  slice buf ={};
  int N =50;
  write_table(&names, &buf, N, 256);
  
  reader rd;
  block_source source = {};
  block_source_from_slice(&source, &buf);

  int err = init_reader(&rd, source);
  assert(err == 0);

  iterator it = {};
  err = reader_seek_ref(&rd, &it, "");
  assert(err == 0);

  int j = 0;
  while (true) {
    ref_record ref = {};
    int r = iterator_next_ref(it, &ref);
    assert(r >= 0);
    if (r > 0) {
      break;
    }
    assert(0 == strcmp(names[j], ref.ref_name));
    j++;
  }
  assert(j == N);
  iterator_destroy(&it);
  free(slice_yield(&buf));
  for (int i = 0; i < N; i++) {
    free(names[i]);
  }
  free(names);
}

void test_table_read_write_seek(bool index) {
  char **names;
  slice buf ={};
  int N =50;
  write_table(&names, &buf, N, 256);
  
  reader rd;
  block_source source = {};
  block_source_from_slice(&source, &buf);

  int err = init_reader(&rd, source);
  assert(err == 0);

  if (!index) {
    rd.ref_offsets.index_offset = 0;
  }
      
  for (int i = 1; i < N; i++) {
    iterator it = {};
    int err = reader_seek_ref(&rd, &it, names[i]);
    assert(err == 0);

    ref_record ref = {};
    err = iterator_next_ref(it, &ref);
    assert(err == 0);
    assert(0 == strcmp(names[i], ref.ref_name));
    assert(i == ref.value[0]);

    ref_record_clear(&ref);
    iterator_destroy(&it);
  }

  free(slice_yield(&buf));
  for (int i = 0; i < N; i++) {
    free(names[i]);
  }
  free(names);
}

void test_table_read_write_seek_linear(void) {
  test_table_read_write_seek(false);
}

void test_table_read_write_seek_index(void) {
  test_table_read_write_seek(true);
}

int main() {
  add_test_case("test_buffer", &test_buffer);
  add_test_case("test_table_read_write_sequential", &test_table_read_write_sequential);
  add_test_case("test_table_read_write_seek_linear", &test_table_read_write_seek_linear);
  add_test_case("test_table_read_write_seek_index", &test_table_read_write_seek_index);
  test_main();
}
