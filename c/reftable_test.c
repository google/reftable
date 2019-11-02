#include <string.h>

#include "api.h"
#include "basics.h"
#include "block.h"
#include "record.h"
#include "test_framework.h"
#include "bytes.h"
#include "writer.h"
#include "reader.h"

void test_table_read_write() {
  buffer buf  = {};
  
  const int N = 50;
  char *names[N];

  write_options opts = {
			.block_size = 256,			
  };

  writer* w  = new_writer(&buffer_write, &buf, &opts);

  {
  ref_record ref = {};
  for (int i = 0; i < N; i++) {
    byte hash[20];
    memset(hash, i, sizeof(hash));
      
    char name[100];
    sprintf(name, "branch%02d", i);

    ref.ref_name = name;
    ref.value = hash;
    names[i] = strdup(name);
    
    int n = writer_add_ref(w, &ref);
    assert(n > 0);
  }
  }
  int n = writer_close(w);
  assert(n > 0);

  reader rd;
  block_source source = {};
  block_source_from_buffer(&source, &buf);
    
  init_reader(&rd, source);
  
  ref_record ref = {};
  record rec= {};
  
  record_from_ref(&rec, &ref);

  iterator it = {};
  int err = reader_seek(&rd, &it, rec);
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
}

int main() {
  add_test_case("test_table_read_write", &test_table_read_write);
  test_main();
}
