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

#include <unistd.h>
#include <string.h>

#include "stack.h"

#include "api.h"
#include "basics.h"
#include "record.h"
#include "test_framework.h"

void test_read_file(void) {
  char fn[256] = "/tmp/stack.XXXXXX";
  int fd = mkstemp(fn);
  assert(fd > 0);

  char out[1024] = "line1\n\nline2\nline3";
  
  int n = write(fd, out, strlen(out));
  assert(n == strlen(out));
  int err = close(fd);
  assert(err >= 0);

  char **names;
  err = read_lines(fn, &names);
  assert(err == 0);

  char *want[] = {"line1", "line2", "line3"};
  int i = 0;
  while (*names != NULL) {
    assert(0 == strcmp(want[i], names[0]));
    i++;
    names ++;
  }
}

int main() {
  add_test_case("test_read_file", &test_read_file);
  test_main();
}
