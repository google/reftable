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
#include <string.h> 

#include "api.h"
#include "iter.h"
#include "block.h"
#include "reader.h"

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

void filtering_ref_iterator_close(void *iter_arg) {
  filtering_ref_iterator* fri = (filtering_ref_iterator*)iter_arg;
  iterator_destroy(&fri->it);
}

int filtering_ref_iterator_next(void *iter_arg, record rec) {
  filtering_ref_iterator * fri = (filtering_ref_iterator*) iter_arg;
  ref_record *ref = (ref_record*) rec.data;

  while(true) {
    int err = iterator_next_ref(fri->it, ref);
    if (err !=  0) {
	return err;
    }

    if (fri->double_check) {
      iterator it = {};

      int err = reader_seek_ref(fri->r, &it, ref->ref_name);
      if (err == 0) {
	err = iterator_next_ref(it, ref);
      }

      iterator_destroy(&it);

      if (err < 0) {
	return err;
      }

      if (err > 0) {
	continue;
      }
    }

    if ((ref->target_value != NULL && 0 == memcmp(fri->oid, ref->target_value, HASH_SIZE))
	|| (ref->value != NULL && 0 == memcmp(fri->oid, ref->value, HASH_SIZE))) {
      return 0;
    }
  }
}
    
struct _iterator_ops filtering_ref_iterator_ops = {
    .next = &filtering_ref_iterator_next,
    .close = &filtering_ref_iterator_close,
};

void iterator_from_filtering_ref_iterator(iterator *it, filtering_ref_iterator *fri) {
  it->iter_arg = fri;
  it->ops = &filtering_ref_iterator_ops;
}

void indexed_table_ref_iter_close(void *p) {
  indexed_table_ref_iter *it = (indexed_table_ref_iter*) p;
  reader_return_block(it->r, &it->block_reader.block);
}

int indexed_table_ref_iter_next_block(indexed_table_ref_iter *it) {
  if (it->offset_idx == it->offset_len) {
    it->finished = true;
    return 1;
  }

  reader_return_block(it->r, &it->block_reader.block);
  
  uint64 off = it->offsets[it->offset_idx++];

  int err = reader_init_block_reader(it->r, &it->block_reader, off, BLOCK_TYPE_REF);
  if (err < 0 ){
    return err;
  }
  if (err > 0){
    // indexed block does not exist.
    return FORMAT_ERROR;
  }

  block_reader_start(&it->block_reader, &it->cur);
  return 0;
}

int indexed_table_ref_iter_next(void *p, record rec) {
  indexed_table_ref_iter *it = (indexed_table_ref_iter*)p;
  ref_record *ref = (ref_record*) rec.data;

  while(true) {
    int err = block_iter_next(&it->cur, rec);
    if (err <0 ) {
      return err;
    }

    if (err > 0) {
      err = indexed_table_ref_iter_next_block(it);
      if (err < 0) {
	return err;
      }

      if (it->finished) {
	return 1;
      }
      continue;
    }

    if (0 == memcmp(it->oid, ref->target_value, HASH_SIZE)
	|| 0 == memcmp(it->oid, ref->value, HASH_SIZE)) {
      return 0;
    }
  }
}

int new_indexed_table_ref_iter(indexed_table_ref_iter **dest,
			       reader * r,
			       byte *oid,
			       uint64 *offsets,
			       int offset_len) {
  indexed_table_ref_iter* itr = calloc(sizeof(indexed_table_ref_iter), 1);
  itr->r = r;
  itr->oid = oid;

  itr->offsets = offsets;
  itr->offset_len = offset_len;
  int err = indexed_table_ref_iter_next_block(itr);
  if (err < 0) {
    free(itr);
  } else {
    *dest = itr;
  }
  return err;
}

struct _iterator_ops indexed_table_ref_iter_ops = {
    .next = &indexed_table_ref_iter_next,
    .close = &indexed_table_ref_iter_close,
};

void iterator_from_indexed_table_ref_iter(iterator *it, indexed_table_ref_iter *itr) {
  it->iter_arg = itr;
  it->ops = &indexed_table_ref_iter_ops;
}

