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

#include <stdlib.h>

#include "api.h"
#include "iter.h"

int empty_iterator_next(void *arg, record rec) { return 1; }

void empty_iterator_close(void *arg) {}

struct _iterator_ops empty_ops = {
    .next = &empty_iterator_next,
    .close = &empty_iterator_close,
};

void iterator_set_empty(iterator *it) {
  it->iter_arg = NULL;
  it->ops = &empty_ops;
}

int iterator_next(iterator it, record rec) {
  return it.ops->next(it.iter_arg, rec);
}

void iterator_destroy(iterator *it) {
  it->ops->close(it->iter_arg);
  it->ops = NULL;
  free(it->iter_arg);
  it->iter_arg = NULL;
}

int iterator_next_ref(iterator it, ref_record *ref) {
  record rec = {} ;
  record_from_ref(&rec, ref);
  return iterator_next(it, rec);
}
