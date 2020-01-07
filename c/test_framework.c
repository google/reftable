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

#include "test_framework.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "constants.h"

struct test_case **test_cases;
int test_case_len;
int test_case_cap;

struct test_case *new_test_case(const char *name, void (*testfunc)()) {
  struct test_case *tc = malloc(sizeof(struct test_case));
  tc->name = name;
  tc->testfunc = testfunc;
  return tc;
}

struct test_case *add_test_case(const char *name, void (*testfunc)()) {
  struct test_case *tc = new_test_case(name, testfunc);
  if (test_case_len == test_case_cap) {
    test_case_cap = 2 * test_case_cap + 1;
    test_cases = realloc(test_cases, sizeof(struct test_case) * test_case_cap);
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

void set_test_hash(byte *p, int i) { memset(p, (byte)i, SHA1_SIZE); }

void print_names(char **a) {
  if (a == NULL || *a == NULL) {
    puts("[]");
    return;
  }
  puts("[");
  char **p = a;
  while (*p) {
    puts(*p);
    p++;
  }
  puts("]");
}
