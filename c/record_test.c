
#include "api.h"
#include "record.h"
#include "test_framework.h"

void varint_roundtrip() {
  uint64 inputs[]  = {0, 1, 27, 127, 128, 257, 4096, ((uint64)1 << 63), ((uint64)1<<63) + ((uint64)1 << 63) - 1};
  for (int i = 0; i < ARRAYSIZE(inputs); i++) {
    byte dest[10];
    
    slice out = { .buf = dest, .len = 10, .cap = 10 };

    uint64 in = inputs[i];
    int n = put_var_int(out, in);
    assert(n > 0);
    out.len = n;

    uint64 got;
    n = get_var_int(&got, out);
    assert(n > 0);
    
    assert(got == in);
  }
}

int main() {
  add_test_case("varint_roundtrip", &varint_roundtrip);
  test_main();
}
  
