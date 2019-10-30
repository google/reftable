#include <string.h>

#include "block.h"
#include "basics.h"
#include "api.h"
#include "record.h"
#include "test_framework.h"


typedef struct {
  int key;
  int *arr;
} binsearch_args;

int binsearch_func(int i, void* void_args) {
  binsearch_args *args = (binsearch_args*)void_args;

  return args->key < args->arr[i];
}

void test_binsearch() {
  int arr[] = { 2, 4, 6, 8, 10};
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
	assert(args.key >= arr[res-1]);
      }
    } else {
      assert(args.key == 10 || args.key == 11);
    }
  }
}

void test_block_read_write() {
  const int header_off = 21; // random
  const int N = 30;
  char * names[N];
  const int block_size = 1024;
  byte * block = calloc(block_size ,1);
  
  block_writer *bw = new_block_writer(BLOCK_TYPE_REF, block, block_size, header_off);
  record *rec = new_record(BLOCK_TYPE_REF);
  ref_record *ref = (ref_record*) rec;

  for (int i = 0  ; i  < N; i++ ) {
    char name[100];
    sprintf(name, "branch%02d", i);
    
    ref->ref_name = strdup(name);
    names[i] = ref->ref_name;
    int n = block_writer_add(bw, rec);
    assert(n > 0);
  }

  int n = block_writer_finish(bw);
  assert(n > 0);

  block_reader *br = new_block_reader(block, header_off, block_size);

  block_iter it = {};
  block_reader_start(br, &it);

  int j =0;
  while (true) {
    int r = block_iter_next(&it, rec);
    assert(r >= 0);
    if (r > 0 ) {
      break;
    }
    assert(0 == strcmp(names[j], ref->ref_name));
    j++;
  }

  slice want = {};
  for (int i = 0; i < N; i++) {
    slice_set_string(&want, names[i]);
    int n = block_reader_seek(br, &it, want);
    assert(n == 0);

    n = block_iter_next(&it, (record*)ref);
    assert(n == 0);
    
    assert(strcmp(names[i], ref->ref_name)== 0);

    want.len--;
    n = block_reader_seek(br, &it, want);
    assert(n == 0);

    n = block_iter_next(&it, (record*)ref);
    assert(n == 0);
    assert(strcmp(names[10* (i/10)], ref->ref_name)== 0);
  }

  free(block);
  free(slice_yield(&want));
  ref->ops->free(rec);
  free(ref);
  for (int i = 0; i < N; i++) {
    free(names[i]);
  }
}


int main() {
  add_test_case("binsearch", &test_binsearch);
  add_test_case("block_read_write", &test_block_read_write);
  test_main();
}
  
