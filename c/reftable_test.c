/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "reftable.h"

#include <string.h>

#include "basics.h"
#include "block.h"
#include "constants.h"
#include "reader.h"
#include "record.h"
#include "test_framework.h"

static const int update_index = 5;

void test_buffer(void) {
  struct slice buf = {};

  byte in[] = "hello";
  slice_write(&buf, in, sizeof(in));
  struct block_source source;
  block_source_from_slice(&source, &buf);
  assert(block_source_size(source) == 6);
  struct block out = {};
  int n = block_source_read_block(source, &out, 0, sizeof(in));
  assert(n == sizeof(in));
  assert(0 == memcmp(in, out.data, n));
  block_source_return_block(source, &out);

  n = block_source_read_block(source, &out, 1, 2);
  assert(n == 2);
  assert(0 == memcmp(out.data, "el", 2));

  block_source_return_block(source, &out);
  block_source_close(&source);
  free(slice_yield(&buf));
}

void write_table(char ***names, struct slice *buf, int N, int block_size) {
  *names = calloc(sizeof(char *), N + 1);

  struct write_options opts = {
      .block_size = block_size,
  };

  struct writer *w = new_writer(&slice_write_void, buf, &opts);

  writer_set_limits(w, update_index, update_index);
  {
    struct ref_record ref = {};
    for (int i = 0; i < N; i++) {
      byte hash[SHA1_SIZE];
      set_test_hash(hash, i);

      char name[100];
      sprintf(name, "refs/heads/branch%02d", i);

      ref.ref_name = name;
      ref.value = hash;
      ref.update_index = update_index;
      (*names)[i] = strdup(name);

      fflush(stdout);
      int n = writer_add_ref(w, &ref);
      assert(n == 0);
    }
  }
  int n = writer_close(w);
  assert(n == 0);

  struct stats *stats = writer_stats(w);
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

void test_log_write_read(void) {
  int N = 2;
  char **names = calloc(sizeof(char *), N + 1);

  struct write_options opts = {
      .block_size = 256,
  };

  struct slice buf = {};
  struct writer *w = new_writer(&slice_write_void, &buf, &opts);

  writer_set_limits(w, 0, N);
  {
    struct ref_record ref = {};
    for (int i = 0; i < N; i++) {
      char name[256];
      sprintf(name, "b%02d%0*d", i, 130, 7);
      names[i] = strdup(name);
      puts(name);
      ref.ref_name = name;
      ref.update_index = i;

      int err = writer_add_ref(w, &ref);
      assert_err(err);
    }
  }

  {
    struct log_record log = {};
    for (int i = 0; i < N; i++) {
      byte hash1[SHA1_SIZE], hash2[SHA1_SIZE];
      set_test_hash(hash1, i);
      set_test_hash(hash2, i + 1);

      log.ref_name = names[i];
      log.update_index = i;
      log.old_hash = hash1;
      log.new_hash = hash2;

      int err = writer_add_log(w, &log);
      assert_err(err);
    }
  }

  int n = writer_close(w);
  assert(n == 0);

  struct stats *stats = writer_stats(w);
  assert(stats->log_stats.blocks > 0);
  writer_free(w);
  w = NULL;

  struct block_source source = {};
  block_source_from_slice(&source, &buf);

  struct reader rd = {};
  int err = init_reader(&rd, source, "file.log");
  assert(err == 0);

  {
    struct iterator it = {};
    err = reader_seek_ref(&rd, &it, names[N-1]);
    assert(err == 0);

    struct ref_record ref = {};
    err = iterator_next_ref(it, &ref);
    assert_err(err);

    // end of iteration.
    err = iterator_next_ref(it, &ref);
    assert(0 < err);

    iterator_destroy(&it);
    ref_record_clear(&ref);
  }

  {
    struct iterator it = {};
    err = reader_seek_log(&rd, &it, "");
    assert(err == 0);

    struct log_record log = {};
    int i = 0;
    while (true) {
      int err = iterator_next_log(it, &log);
      if (err > 0) {
        break;
      }

      assert_err(err);
      assert_streq(names[i], log.ref_name);
      assert(i == log.update_index);
      i++;
    }

    assert(i == N);
    iterator_destroy(&it);
  }

  // cleanup.
  free(slice_yield(&buf));
  free_names(names);
  reader_close(&rd);
}

void test_table_read_write_sequential(void) {
  char **names;
  struct slice buf = {};
  int N = 50;
  write_table(&names, &buf, N, 256);

  struct block_source source = {};
  block_source_from_slice(&source, &buf);

  struct reader rd = {};
  int err = init_reader(&rd, source, "file.ref");
  assert(err == 0);

  struct iterator it = {};
  err = reader_seek_ref(&rd, &it, "");
  assert(err == 0);

  int j = 0;
  while (true) {
    struct ref_record ref = {};
    int r = iterator_next_ref(it, &ref);
    assert(r >= 0);
    if (r > 0) {
      break;
    }
    assert(0 == strcmp(names[j], ref.ref_name));
    assert(update_index == ref.update_index);

    j++;
    ref_record_clear(&ref);
  }
  assert(j == N);
  iterator_destroy(&it);
  free(slice_yield(&buf));
  free_names(names);

  reader_close(&rd);
}

void test_table_write_small_table(void) {
  char **names;
  struct slice buf = {};
  int N = 1;
  write_table(&names, &buf, N, 4096);
  assert(buf.len < 200);
  free(slice_yield(&buf));
  free_names(names);
}

void test_table_read_api(void) {
  char **names;
  struct slice buf = {};
  int N = 50;
  write_table(&names, &buf, N, 256);

  struct reader rd = {};
  struct block_source source = {};
  block_source_from_slice(&source, &buf);

  int err = init_reader(&rd, source, "file.ref");
  assert(err == 0);

  struct iterator it = {};
  err = reader_seek_ref(&rd, &it, names[0]);
  assert(err == 0);

  struct log_record log = {};
  err = iterator_next_log(it, &log);
  assert(err == API_ERROR);

  free(slice_yield(&buf));
  for (int i = 0; i < N; i++) {
    free(names[i]);
  }
  free(names);
  reader_close(&rd);
}

void test_table_read_write_seek(bool index) {
  char **names;
  struct slice buf = {};
  int N = 50;
  write_table(&names, &buf, N, 256);

  struct reader rd = {};
  struct block_source source = {};
  block_source_from_slice(&source, &buf);

  int err = init_reader(&rd, source, "file.ref");
  assert(err == 0);

  if (!index) {
    rd.ref_offsets.index_offset = 0;
  }

  for (int i = 1; i < N; i++) {
    struct iterator it = {};
    int err = reader_seek_ref(&rd, &it, names[i]);
    assert(err == 0);
    struct ref_record ref = {};
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
  reader_close(&rd);
}

void test_table_read_write_seek_linear(void) {
  test_table_read_write_seek(false);
}

void test_table_read_write_seek_index(void) {
  test_table_read_write_seek(true);
}

void test_table_refs_for(bool indexed) {
  int N = 50;

  char **want_names = calloc(sizeof(char *), N + 1);

  int want_names_len = 0;
  byte want_hash[SHA1_SIZE];
  set_test_hash(want_hash, 4);

  struct write_options opts = {
      .block_size = 256,
  };

  struct slice buf = {};
  struct writer *w = new_writer(&slice_write_void, &buf, &opts);
  {
    struct ref_record ref = {};
    for (int i = 0; i < N; i++) {
      byte hash[SHA1_SIZE];
      memset(hash, i, sizeof(hash));
      char fill[51] = {};
      memset(fill, 'x', 50);
      char name[100];
      // Put the variable part in the start
      sprintf(name, "br%02d%s", i, fill);
      name[40] = 0;
      ref.ref_name = name;

      byte hash1[SHA1_SIZE];
      byte hash2[SHA1_SIZE];

      set_test_hash(hash1, i / 4);
      set_test_hash(hash2, 3 + i / 4);
      ref.value = hash1;
      ref.target_value = hash2;

      // 80 bytes / entry, so 3 entries per block. Yields 17 blocks.
      int n = writer_add_ref(w, &ref);
      assert(n == 0);

      if (0 == memcmp(hash1, want_hash, SHA1_SIZE) ||
          0 == memcmp(hash2, want_hash, SHA1_SIZE)) {
        want_names[want_names_len++] = strdup(name);
      }
    }
  }

  int n = writer_close(w);
  assert(n == 0);

  writer_free(w);
  w = NULL;

  struct reader rd;
  struct block_source source = {};
  block_source_from_slice(&source, &buf);

  int err = init_reader(&rd, source, "file.ref");
  assert(err == 0);
  if (!indexed) {
    rd.obj_offsets.present = 0;
  }

  struct iterator it = {};
  err = reader_seek_ref(&rd, &it, "");
  assert(err == 0);
  iterator_destroy(&it);

  err = reader_refs_for(&rd, &it, want_hash, SHA1_SIZE);
  assert(err == 0);

  struct ref_record ref = {};

  int j = 0;
  while (true) {
    int err = iterator_next_ref(it, &ref);
    assert(err >= 0);
    if (err > 0) {
      break;
    }

    assert(j < want_names_len);
    assert(0 == strcmp(ref.ref_name, want_names[j]));
    j++;
    ref_record_clear(&ref);
  }
  assert(j == want_names_len);

  free(slice_yield(&buf));
  free_names(want_names);
  iterator_destroy(&it);
  reader_close(&rd);
}

void test_table_refs_for_no_index(void) { test_table_refs_for(false); }

void test_table_refs_for_obj_index(void) { test_table_refs_for(true); }

int main() {
  add_test_case("test_log_write_read", test_log_write_read);
  add_test_case("test_table_write_small_table", &test_table_write_small_table);
  add_test_case("test_buffer", &test_buffer);
  add_test_case("test_table_read_api", &test_table_read_api);
  add_test_case("test_table_read_write_sequential",
                &test_table_read_write_sequential);
  add_test_case("test_table_read_write_seek_linear",
                &test_table_read_write_seek_linear);
  add_test_case("test_table_read_write_seek_index",
                &test_table_read_write_seek_index);
  add_test_case("test_table_read_write_refs_for_no_index",
                &test_table_refs_for_no_index);
  add_test_case("test_table_read_write_refs_for_obj_index",
                &test_table_refs_for_obj_index);
  test_main();
}
