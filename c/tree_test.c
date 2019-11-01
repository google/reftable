#include "basics.h"
#include "api.h"
#include "record.h"
#include "test_framework.h"
#include "tree.h"

static int test_compare(const void *a, const void*b) {
  return a - b;
}

void test_tree() {
  tree_node* root  = NULL;

  void *values[11] = {};
  tree_node* nodes[11] = {}; 
  int i = 1;
  do {
    nodes[i] = tree_search(values + i, &root, &test_compare, 1);
    i = (i* 7) % 11; 
  } while (i != 1);

  for (int i = 1; i < ARRAYSIZE(nodes); i++) {
    assert(values +i == nodes[i]->key);
    assert(nodes[i] == tree_search(values +i, &root, &test_compare, 0));
  }
}

int main() {
  add_test_case("test_tree", &test_tree);
  test_main();
}
  
