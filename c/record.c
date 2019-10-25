
#include "api.h"

int get_var_int(dest *uint64, in slice) {
  if (in.len == 0) {
    return -1;
  }

  int ptr = 0;
  uint64 val = in.p[0];
}
