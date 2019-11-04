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

#ifndef ITER_H
#define ITER_H

#include "record.h"

struct _iterator_ops {
  int (*next)(void *iter_arg, record rec);
  void (*close)(void *iter_arg);
};

void iterator_set_empty(iterator *it);
int iterator_next(iterator it, record rec);

#endif
