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

#include "slice.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "api.h"

#include "api.h"
#include "basics.h"
#include "record.h"
#include "test_framework.h"

void test_slice(void) {
  struct slice s = {};
  slice_set_string(&s, "abc");
  assert(0 == strcmp("abc", slice_as_string(&s)));

  struct slice t = {};
  slice_set_string(&t, "pqr");

  slice_append(&s, t);
  assert(0 == strcmp("abcpqr", slice_as_string(&s)));

  free(slice_yield(&s));
  free(slice_yield(&t));
}


int main() {
  add_test_case("test_slice", &test_slice);
  test_main();
}
