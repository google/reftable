#include <stdlib.h>

#include "tree.h"

tree_node *tree_search(void *key, tree_node **rootp,
		  int (*compare)(const void *, const void *),
		  int insert) {
  if (*rootp == NULL) {
    if (!insert) {
      return NULL;
    } else {
      tree_node *n = calloc(sizeof(tree_node),1 );
      n->key = key;
      *rootp = n;
      return *rootp;
    }
  }

  tree_node* n = *rootp;
  int res = compare(key, n->key);
  if (res < 0) {
    return tree_search(key, &n->left, compare, insert);
  } else if (res > 0) {
    return tree_search(key, &n->right, compare, insert);
  }
  return n;
}

void infix_walk(tree_node*t, void (*action)(void *arg, void *key), void*arg) {
  if (t->left!=NULL) {
    infix_walk(t->left, action, arg);
  }
  action(arg, t->key);
  if (t->right!=NULL) {
    infix_walk(t->right, action, arg);
  }
}
