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

#ifndef TREE_H
#define TREE_H

typedef struct _tree_node tree_node;

typedef struct _tree_node {
  void *key;
  tree_node *left, *right;
} tree_node;

tree_node *tree_search(void *key, tree_node **rootp,
                       int (*compare)(const void *, const void *), int insert);
void infix_walk(tree_node *t, void (*action)(void *arg, void *key), void *arg);

#endif
