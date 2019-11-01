#ifndef TREE_H
#define TREE_H

typedef struct _tree_node tree_node;

typedef struct _tree_node {
  void *key;
  tree_node *left, *right;
} tree_node;

tree_node *tree_search(void *key, tree_node **rootp,
		       int (*compare)(const void *, const void *),
		       int insert);

#endif
