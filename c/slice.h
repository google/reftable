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

#ifndef SLICE_H
#define SLICE_H

#include "api.h"
#include "basics.h"

struct slice {
  byte *buf;
  int len;
  int cap;
};

void slice_set_string(struct slice *dest, const char *);
char *slice_to_string(struct slice src);
bool slice_equal(struct slice a, struct slice b);
byte *slice_yield(struct slice *s);
void slice_copy(struct slice *dest, struct slice src);
void slice_resize(struct slice *s, int l);
int slice_compare(struct slice a, struct slice b);
int slice_write(struct slice *b, byte *data, int sz);
int slice_write_void(void *b, byte *data, int sz);
struct block_source;
void block_source_from_slice(struct block_source *bs, struct slice *buf);

#endif
