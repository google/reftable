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

#ifndef TEST_FRAMEWORK_H
#define TEST_FRAMEWORK_H

#include <stdio.h>
#include <stdlib.h>

#include "api.h"

#ifdef NDEBUG
#undef NDEBUG
#endif

#include <assert.h>

#ifdef assert
#undef assert
#endif

#define assert_err(c)                                                           \
  if (c != 0) { \
    fflush(stderr);                                                         \
    fflush(stdout);                                                         \
    fprintf(stderr, "%s: %d: error == %d, want 0\n", __FILE__, __LINE__, c); \
    abort();                                                                \
  }

#define assert(c)                                                           \
  if (!(c)) {                                                               \
    fflush(stderr);                                                         \
    fflush(stdout);                                                         \
    fprintf(stderr, "%s: %d: failed assertion %s\n", __FILE__, __LINE__, #c); \
    abort();                                                                \
  }

struct test_case {
  const char *name;
  void (*testfunc)();
};

struct test_case *new_test_case(const char *name, void (*testfunc)());
struct test_case *add_test_case(const char *name, void (*testfunc)());
void test_main();

void set_test_hash(byte *p, int i) ;

#endif
