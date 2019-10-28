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
  const int N = 30;
  char * names[N];
  byte * block  = calloc(1024, 1);
  
  block_writer *bw = new_block_writer(BLOCK_TYPE_REF, block, 1024, 0);
  record *rec = new_record(BLOCK_TYPE_REF);
  ref_record *ref = (ref_record*) rec;

  for (int i = 0  ; i  < 30; i++ ) {
    char name[100];
    sprintf(name, "branch%02d", i);
    
    ref->ref_name = strdup(name);
    names[i] = ref->ref_name;
    int n = block_writer_add(bw, rec);
    assert(n > 0);
  }

  int n = block_writer_finish(bw);
  assert(n > 0);

  block_reader *br = new_block_reader(block, 0, 1024);

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
}


int main() {
  add_test_case("binsearch", &test_binsearch);
  add_test_case("block_read_write", &test_block_read_write);
  test_main();
}
  
