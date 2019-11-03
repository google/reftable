#include <string.h>

#include "api.h"
#include "basics.h"
#include "block.h"
#include "record.h"
#include "test_framework.h"

typedef struct {
  int key;
  int *arr;
} binsearch_args;

int binsearch_func(int i, void *void_args) {
  binsearch_args *args = (binsearch_args *)void_args;

  return args->key < args->arr[i];
}

void test_binsearch() {
  int arr[] = {2, 4, 6, 8, 10};
  int sz = ARRAYSIZE(arr);
  binsearch_args args = {
      .arr = arr,
  };

  for (int i = 1; i < 11; i++) {
    args.key = i;
    int res = binsearch(sz, &binsearch_func, &args);

    if (res < sz) {
      assert(args.key < arr[res]);
      if (res > 0) {
        assert(args.key >= arr[res - 1]);
      }
    } else {
      assert(args.key == 10 || args.key == 11);
    }
  }
}

void test_block_read_write() {
  const int header_off = 21; // random
  const int N = 30;
  char *names[N];
  const int block_size = 1024;
  byte *block = calloc(block_size, 1);

  block_writer bw = {};
  block_writer_init(&bw, BLOCK_TYPE_REF, block, block_size, header_off);
  ref_record ref = {};
  record rec = {};
  record_from_ref(&rec, &ref);

  for (int i = 0; i < N; i++) {
    char name[100];
    sprintf(name, "branch%02d", i);

    byte hash[20];
    memset(hash, i, sizeof(hash));
      
    ref.ref_name = name;
    ref.value = hash;
    names[i] = strdup(name);
    int n = block_writer_add(&bw, rec);
    ref.ref_name = NULL;
    ref.value = NULL;
    assert(n == 0);
  }

  int n = block_writer_finish(&bw);
  assert(n > 0);

  block_writer_clear(&bw);
  
  block_reader br = {};
  block_reader_init(&br, block, header_off, block_size);

  block_iter it = {};
  block_reader_start(&br, &it);

  int j = 0;
  while (true) {
    int r = block_iter_next(&it, rec);
    assert(r >= 0);
    if (r > 0) {
      break;
    }
    assert(0 == strcmp(names[j], ref.ref_name));
    j++;
  }
  record_clear(rec);
  block_iter_close(&it);

  slice want = {};
  for (int i = 0; i < N; i++) {
    slice_set_string(&want, names[i]);
    
    block_iter it = {};
    int n = block_reader_seek(&br, &it, want);
    assert(n == 0);
    
    n = block_iter_next(&it, rec);
    assert(n == 0);

    assert(strcmp(names[i], ref.ref_name) == 0);

    want.len--;
    n = block_reader_seek(&br, &it, want);
    assert(n == 0);

    n = block_iter_next(&it, rec);
    assert(n == 0);
    assert(strcmp(names[10 * (i / 10)], ref.ref_name) == 0);

    block_iter_close(&it);
  }

  record_clear(rec);
  free(block);
  free(slice_yield(&want));
  for (int i = 0; i < N; i++) {
    free(names[i]);
  }
}

int main() {
  add_test_case("binsearch", &test_binsearch);
  add_test_case("block_read_write", &test_block_read_write);
  test_main();
}
