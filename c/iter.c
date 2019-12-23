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

#include "iter.h"

#include <stdlib.h>
#include <string.h>

#include "reftable.h"
#include "constants.h"
#include "block.h"
#include "reader.h"

bool iterator_is_null(struct iterator it) { return it.ops == NULL; }

int empty_iterator_next(void *arg, struct record rec) { return 1; }

void empty_iterator_close(void *arg) {}

struct iterator_vtable empty_vtable = {
    .next = &empty_iterator_next,
    .close = &empty_iterator_close,
};

void iterator_set_empty(struct iterator *it) {
  it->iter_arg = NULL;
  it->ops = &empty_vtable;
}

int iterator_next(struct iterator it, struct record rec) {
  return it.ops->next(it.iter_arg, rec);
}

void iterator_destroy(struct iterator *it) {
  if (it->ops == NULL) {
    return;
  }
  it->ops->close(it->iter_arg);
  it->ops = NULL;
  free(it->iter_arg);
  it->iter_arg = NULL;
}

int iterator_next_ref(struct iterator it, struct ref_record *ref) {
  struct record rec = {};
  record_from_ref(&rec, ref);
  return iterator_next(it, rec);
}

void filtering_ref_iterator_close(void *iter_arg) {
  struct filtering_ref_iterator *fri =
      (struct filtering_ref_iterator *)iter_arg;
  iterator_destroy(&fri->it);
}

int filtering_ref_iterator_next(void *iter_arg, struct record rec) {
  struct filtering_ref_iterator *fri =
      (struct filtering_ref_iterator *)iter_arg;
  struct ref_record *ref = (struct ref_record *)rec.data;

  while (true) {
    int err = iterator_next_ref(fri->it, ref);
    if (err != 0) {
      return err;
    }

    if (fri->double_check) {
      struct iterator it = {};

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

    if ((ref->target_value != NULL &&
         0 == memcmp(fri->oid, ref->target_value, HASH_SIZE)) ||
        (ref->value != NULL && 0 == memcmp(fri->oid, ref->value, HASH_SIZE))) {
      return 0;
    }
  }
}

struct iterator_vtable filtering_ref_iterator_vtable = {
    .next = &filtering_ref_iterator_next,
    .close = &filtering_ref_iterator_close,
};

void iterator_from_filtering_ref_iterator(struct iterator *it,
                                          struct filtering_ref_iterator *fri) {
  it->iter_arg = fri;
  it->ops = &filtering_ref_iterator_vtable;
}

void indexed_table_ref_iter_close(void *p) {
  struct indexed_table_ref_iter *it = (struct indexed_table_ref_iter *)p;
  reader_return_block(it->r, &it->block_reader.block);
}

int indexed_table_ref_iter_next_block(struct indexed_table_ref_iter *it) {
  if (it->offset_idx == it->offset_len) {
    it->finished = true;
    return 1;
  }

  reader_return_block(it->r, &it->block_reader.block);

  uint64_t off = it->offsets[it->offset_idx++];

  int err =
      reader_init_block_reader(it->r, &it->block_reader, off, BLOCK_TYPE_REF);
  if (err < 0) {
    return err;
  }
  if (err > 0) {
    // indexed block does not exist.
    return FORMAT_ERROR;
  }

  block_reader_start(&it->block_reader, &it->cur);
  return 0;
}

int indexed_table_ref_iter_next(void *p, struct record rec) {
  struct indexed_table_ref_iter *it = (struct indexed_table_ref_iter *)p;
  struct ref_record *ref = (struct ref_record *)rec.data;

  while (true) {
    int err = block_iter_next(&it->cur, rec);
    if (err < 0) {
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

    if (0 == memcmp(it->oid, ref->target_value, HASH_SIZE) ||
        0 == memcmp(it->oid, ref->value, HASH_SIZE)) {
      return 0;
    }
  }
}

int new_indexed_table_ref_iter(struct indexed_table_ref_iter **dest,
                               struct reader *r, byte *oid, uint64_t *offsets,
                               int offset_len) {
  struct indexed_table_ref_iter *itr =
      calloc(sizeof(struct indexed_table_ref_iter), 1);
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

struct iterator_vtable indexed_table_ref_iter_vtable = {
    .next = &indexed_table_ref_iter_next,
    .close = &indexed_table_ref_iter_close,
};

void iterator_from_indexed_table_ref_iter(struct iterator *it,
                                          struct indexed_table_ref_iter *itr) {
  it->iter_arg = itr;
  it->ops = &indexed_table_ref_iter_vtable;
}
