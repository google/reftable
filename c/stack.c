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

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "stack.h"

void stack_free(struct stack *st) {
  free(st->list_file);
  free(st->reftable_dir);
  merged_table_free(st->merged);
  st->merged = NULL;
  st->list_file = NULL;
  st->reftable_dir = NULL;
}

int stack_reload(struct stack *st) {
  assert(false);
  return -1;
}

int new_stack(struct stack **dest, const char *dir,
	      const char *list_file,
	      struct write_options cfg) {
  *dest = NULL;
  struct stack *p = calloc(sizeof(struct stack),1);
  p->list_file = strdup(list_file);
  p->reftable_dir = strdup(dir);
  p->cfg = cfg;
  int err = stack_reload(p);
  if (err < 0) {
    stack_free(p);
  } else {
    *dest = p;
  }
  return err;
}


int read_lines(const char *filename, char ***namesp) {
  FILE *f = fopen(filename, "r");
  if (f == NULL) {
    int e = errno;
    if (e == EINVAL) {
      *namesp = calloc(sizeof(char *), 1);
      return 0;
    }

    return IO_ERROR;
  }
  char *buf = NULL;
  
  int err = fseek(f, 0, SEEK_END);
  if (err < 0) {
    err = IO_ERROR;
    goto exit;
  }
  long size = ftell(f);
  if (size < 0) {
    err = IO_ERROR;
    goto exit;
  }    
  err = fseek(f, 0, SEEK_SET);
  if (err < 0) {
    err = IO_ERROR;
    goto exit;
  }
  
  buf = malloc(size+1);
  size_t n = fread(buf, 1, size, f);
  if (n != size) {
    printf("HERE %ld %ld\n",n,size);
    return IO_ERROR;
  }
  buf[size] = 0;

  char **names =  NULL;
  int names_cap = 0;
  int names_len = 0;
  
  char *p = buf;
  char *end = buf + size;
  while (p < end) {
    char *next = strchr(p, '\n');
    if (next != NULL) {
      *next = 0;
    } else {
      next = end;
    }
    if (p > next) {
      
      if (names_len == names_cap) {
	names_cap = 2*names_cap + 1;
	*names = realloc(*names, names_cap * sizeof(char*));
      }

      names[names_len++] = strdup(p);
    }
    p = next + 1;
  }

  if (names_len == names_cap) {
    names_cap = 2*names_cap + 1;
    names = realloc(names, names_cap * sizeof(char*));
  }

  names[names_len] = NULL;
  *namesp = names;
  
 exit:
  free(buf);
  fclose(f);
  return err;
}

struct merged_table *stack_merged_table(struct stack *st) {
  return st->merged;
}

void stack_close(struct stack *st) {
  if (st->merged == NULL) {
    return;
  }
  
  for (int i = 0; i < st->merged->stack_len; i++) {
    reader_close(st->merged->stack[i]);
  }

  free(st->merged->stack);
  st->merged->stack = NULL;
  st->merged->stack_len = 0;
}

int stack_reload_once(struct stack* st, char **names) {
  struct reader **cur = calloc(sizeof(struct reader*) *st->merged->stack_len);
  int cur_len = st->merged->stack_len;
  for (int i =0; i < cur_len; i++) {
    cur[i] = st->merged->stack[i];
  }

  int names_len = 0;
  for (char **p = names; *p; p++) {
    names_len ++;
  }

  struct reader **new_tables = malloc(sizeof(struct reader*)*names_len);
  int new_tables_len = 0;

  for (*names) {
    char *name = *names;
    names++;

    struct reader *rd = NULL;
    for (int j = 0; j < cur_len; j++) {
      if (cur[j] !=NULL && 0 == strcmp(cur[j]->name, name)) {
	rd = cur[j];
	cur[j] = NULL;
	break;
      }
    }

    if (rd == NULL) {
      struct block_source src = {};
      err  =block_source_from_file(&src, name);
      if (err < 0) {
	goto exit;
      }

      
      err = new_reader(&rd, src, name);
      if (err < 0) {
	goto exit;
      }
    }

    new_tables[new_table_len++] = rd;
  }

  // success!
  struct merged_table new_merged= NULL;
  err = new_merged_table(&new_merged, new_tables, new_table_len);
  if (err < 0) {
    goto exit;
  }
  
  merged_table_free(st->merged);
  st->merged = new_merged;
  
 exit:
  for (int i = 0; i < new_tables_len; i++) {
    reader_close(new_tables[i]);
  }
  free(new_tables);
  free(cur);
}
