#include <string.h>

#include "api.h"
#include "basics.h"
#include "block.h"
#include "reader.h"
#include "record.h"
#include "test_framework.h"
#include "writer.h"

void test_buffer() {
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

  for (int i = 0; i < w->stats.ref_stats.blocks; i++) {
    int off = i * opts.block_size;
    if (off == 0) {
      off = HEADER_SIZE;
    }
    assert(buf->buf[off] == 'r');
  }

  writer_free(w);
  w = NULL;
}


void test_table_read_write_sequential() {
  char **names;
  slice buf ={};
  int N =50;
  write_table(&names, &buf, N, 256);
  
  reader rd;
  block_source source = {};
  block_source_from_slice(&source, &buf);

  int err = init_reader(&rd, source);
  assert(err == 0);

  ref_record ref = {};
  record rec = {};
  record_from_ref(&rec, &ref);

  iterator it = {};
  err = reader_seek(&rd, &it, rec);
  assert(err == 0);

  int j = 0;
  while (true) {
    int r = iterator_next(it, rec);
    assert(r >= 0);
    if (r > 0) {
      break;
    }
    assert(0 == strcmp(names[j], ref.ref_name));
    j++;
  }
  assert(j == N);
  iterator_destroy(&it);
  record_clear(rec);
  free(slice_yield(&buf));
  for (int i = 0; i < N; i++) {
    free(names[i]);
  }
  free(names);
}

void test_table_read_write_seek() {
  char **names;
  slice buf ={};
  int N =50;
  write_table(&names, &buf, N, 256);
  
  reader rd;
  block_source source = {};
  block_source_from_slice(&source, &buf);

  int err = init_reader(&rd, source);
  assert(err == 0);

  for (int i = 0; i < N; i++) {
    iterator it = {};
    ref_record ref = {};
    record rec = {};
    record_from_ref(&rec, &ref);

    ref.ref_name = names[i];
    int err = reader_seek(&rd, &it, rec);
    assert(err == 0);
    ref.ref_name = NULL;

    err = iterator_next(it, rec);
    assert(err == 0);
    assert(0 == strcmp(names[i], ref.ref_name));
    assert(i == ref.value[0]);

    record_clear(rec);
    iterator_destroy(&it);
  }

  free(slice_yield(&buf));
  for (int i = 0; i < N; i++) {
    free(names[i]);
  }
  free(names);
}

int main() {
  add_test_case("test_buffer", &test_buffer);
  add_test_case("test_table_read_write_sequential", &test_table_read_write_sequential);
  add_test_case("test_table_read_write_seek", &test_table_read_write_seek);
  test_main();
}
