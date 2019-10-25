#include <stdlib.h>
#include <stdio.h>

#include "test_framework.h"

test_case **test_cases;
int test_case_len;
int test_case_cap;

test_case* new_test_case (const char *name, void (*testfunc)()) {
  test_case *tc = malloc(sizeof(test_case));
  tc->name = name;
  tc->testfunc = testfunc;
  return tc;
}

test_case* add_test_case (const char *name, void (*testfunc)()) {
  test_case *tc = new_test_case(name, testfunc);
  if (test_case_len == test_case_cap) {
    test_case_cap = 2*test_case_cap+1;
    test_cases = realloc(test_cases, sizeof(test_case)* test_case_cap);
  }
  
  test_cases[test_case_len++] = tc;
  return tc;
}

void test_main() {
  for (int i = 0; i < test_case_len; i++) {
    printf("case %s\n", test_cases[i]->name);
    test_cases[i]->testfunc();
  }
}
