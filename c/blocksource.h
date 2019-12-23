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

#ifndef BLOCKSOURCE_H
#define BLOCKSOURCE_H

#include "reftable.h"

uint64_t block_source_size(struct block_source source);
int block_source_read_block(struct block_source source, struct block *dest,
                            uint64_t off, uint32_t size);
void block_source_return_block(struct block_source source, struct block *ret);
void block_source_close(struct block_source source);

#endif
