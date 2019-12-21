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

#include "tree.h"

#include "api.h"
#include "basics.h"
#include "record.h"
#include "test_framework.h"

static int test_compare(const void *a, const void *b) { return a - b; }

struct curry {
  void *last;
};

void check_increasing(void *arg, void *key) {
  struct curry *c = (struct curry *)arg;
  if (c->last != NULL) {
    assert(test_compare(c->last, key) < 0);
  }
  c->last = key;
}

void test_tree() {
  struct tree_node *root = NULL;

  void *values[11] = {};
  struct tree_node *nodes[11] = {};
  int i = 1;
  do {
    nodes[i] = tree_search(values + i, &root, &test_compare, 1);
    i = (i * 7) % 11;
  } while (i != 1);

  for (int i = 1; i < ARRAYSIZE(nodes); i++) {
    assert(values + i == nodes[i]->key);
    assert(nodes[i] == tree_search(values + i, &root, &test_compare, 0));
  }

  struct curry c = {};
  infix_walk(root, check_increasing, &c);
  tree_free(root);
}

int main() {
  add_test_case("test_tree", &test_tree);
  test_main();
}
