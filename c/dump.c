// Copyright 2020 Google Inc. All rights reserved.
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

#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "reftable.h"

static int dump_table(const char *tablename) {
  struct block_source src = {};
  int err = block_source_from_file(&src, tablename);
  if (err < 0) {
    return err;
  }

  struct reader *r = NULL;
  err = new_reader(&r, src, tablename);
  if (err < 0) {
    return err;
  }

  {
    struct iterator it = {};
    err = reader_seek_ref(r, &it, "");
    if (err < 0) {
      return err;
    }

    struct ref_record ref = {};
    while (1) {
      err = iterator_next_ref(it, &ref);
      if (err > 0) {
        break;
      }
      if (err < 0) {
        return err;
      }
      ref_record_print(&ref, 20);
    }
    iterator_destroy(&it);
    ref_record_clear(&ref);
  }

  {
    struct iterator it = {};
    err = reader_seek_log(r, &it, "");
    if (err < 0) {
      return err;
    }
    struct log_record log = {};
    while (1) {
      err = iterator_next_log(it, &log);
      if (err > 0) {
        break;
      }
      if (err < 0) {
        return err;
      }
      log_record_print(&log, 20);
    }
    iterator_destroy(&it);
    log_record_clear(&log);
  }
  return 0;
}

int main(int argc, char *argv[]) {
  int opt;
  const char *table = NULL;
  while ((opt = getopt(argc, argv, "t:")) != -1) {
    switch (opt) {
      case 't':
        table = strdup(optarg);
        break;
      case '?':
        printf("usage: %s [-table tablefile]\n", argv[0]);
        return 2;
        break;
    }
  }

  if (table != NULL) {
    int err = dump_table(table);
    if (err < 0) {
      fprintf(stderr, "%s: %s: %s\n", argv[0], table, error_str(err));
      return 1;
    }
  }
  return 0;
}
