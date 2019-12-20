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
#include <sys/time.h>
#include <unistd.h>
       #include <sys/types.h>
       #include <sys/stat.h>
       #include <fcntl.h>

#include "stack.h"
#include "merged.h"
#include "reader.h"

void stack_free(struct stack *st) {
  free(st->list_file);
  free(st->reftable_dir);
  merged_table_free(st->merged);
  st->merged = NULL;
  st->list_file = NULL;
  st->reftable_dir = NULL;
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

  char **names = NULL;
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

  merged_table_free(st->merged);
  st->merged = NULL;

  free(st->list_file);
  st->list_file = NULL;
  free(st->reftable_dir);
  st->reftable_dir = NULL;
}

int stack_reload_once(struct stack* st, char **names) {
  struct reader **cur = calloc(sizeof(struct reader*), st->merged->stack_len);
  int cur_len = st->merged->stack_len;
  for (int i =0; i < cur_len; i++) {
    cur[i] = st->merged->stack[i];
  }

  int err = 0;
  int names_len = 0;
  for (char **p = names; *p; p++) {
    names_len ++;
  }

  struct reader **new_tables = malloc(sizeof(struct reader*)*names_len);
  int new_tables_len = 0;

  while (*names) {
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
      err = block_source_from_file(&src, name);
      if (err < 0) {
	goto exit;
      }

      
      err = new_reader(&rd, src, name);
      if (err < 0) {
	goto exit;
      }
    }

    new_tables[new_tables_len++] = rd;
  }

  // success!
  struct merged_table *new_merged = NULL;
  err = new_merged_table(&new_merged, new_tables, new_tables_len);
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
  return err;
}

struct merged_table *stack_merged(struct stack* st) {
  return st->merged;
}

int tv_cmp(struct timeval *a,struct timeval *b) {
  time_t diff =  a->tv_sec - b->tv_sec;
  if (diff != 0) {
      return diff;
  }
    
  suseconds_t udiff = a->tv_usec - b->tv_usec;
  return udiff;
}

void free_names(char **a) {
  char **p = a;
  while (*p) {
    free(*p);
    p++;
  }
  free(a);
}

int names_equal(char **a, char **b) {
  while (*a && *b) {
    if (0 == strcmp(*a, *b)) {
      return 1;
    }

    a++;
    b++;
  }

  return *a == *b;
}

int stack_reload(struct stack* st) {
  struct timeval deadline;
  int err = gettimeofday(&deadline, NULL);
  if (err < 0) {
    return err;
  }

  deadline.tv_sec += 3;
  int delay = 0;
  while (true) {
    struct timeval tv = {};
    int err = gettimeofday(&tv, NULL);
    if (err < 0) {
      return err;
    }

    if (tv_cmp(&tv, &deadline) >= 0) {
      break;
    }
    
    char **names = NULL;
    err = read_lines(st->list_file, &names);
    if (err < 0) {
      free_names(names);
      return err;
    }

    err = stack_reload_once(st, names);
    if (err == 0) {
      free_names(names);
      break;
    }
    if (err != ERR_NOT_EXIST) {
      free_names(names);
      return err;
    }

    char **names_after = NULL;
    err = read_lines(st->list_file, &names_after);
    if (err < 0) {
      free_names(names);
      return err;
    }

    if (names_equal(names_after, names)) {
      free_names(names);
      free_names(names_after);
      return -1; 
    }
    free_names(names);
    free_names(names_after);

    delay = delay * 2 + 100; // TODO: jitter.
    usleep(delay);
  }

  return 0;
}

// -1 = error
// 0 = up to date
// 1 = changed.
int stack_uptodate(struct stack *st) {
    char **names = NULL;
    int err = read_lines(st->list_file, &names);
    if (err < 0) {
      return err;
    }
    
    for (int i = 0; i < st->merged->stack_len; i++) {
      if (names[i] == NULL) {
	err = 1;
	goto exit;
      }
      
      if (0 != strcmp(st->merged->stack[i]->name, names[i])) {
	err = 1;
	goto exit;
      }
    }

    if (names[st->merged->stack_len] != NULL) {
      err = 1;
      goto exit;
    }

 exit:
    free_names(names);
    return err;
}

int stack_add(struct stack* st, int (*write)(struct writer *wr, void*arg), void *arg) {
  int err = stack_try_add(st, write, arg);
  if (err < 0) {
    if (err == LOCK_FAILURE) {
      err = stack_reload(st);
    }
    return err;
  }

  return 0 ; //  stack_autocompact(st);
}

void format_name(struct slice *dest, uint64_t min, uint64_t max) {
  char buf [1024];
  sprintf(buf, "%012lx-%012lx", min, max);
  slice_set_string(dest, buf);
}

int stack_try_add(struct stack* st, int (*write_table)(struct writer *wr, void*arg), void *arg) {
  struct slice lock_name = {};
  slice_set_string(&lock_name, st->list_file);
  struct slice lock_suffix = {};
  slice_set_string(&lock_suffix, ".lock");
  slice_append(&lock_name, lock_suffix);
  struct slice temp_tab_name = {};

  int err = 0;
  int fd = 0;
  fd = open(slice_as_string(&lock_name), O_EXCL|O_CREAT|O_WRONLY, 0644);
  if (fd < 0) {
    if (errno == EEXIST) {
      err = LOCK_FAILURE;
      goto exit;
    }
    err = -1;
    goto exit;
  }

  err = stack_uptodate(st);
  if (err < 0) {
    goto exit;
  }

  if (err > 1) {
    err = LOCK_FAILURE;
    goto exit;
  }

  for (int i = 0; i < st->merged->stack_len; i++) {
   char buf[1024];
   int n = sprintf(buf, "%s\n", st->merged->stack[i]->name);
   n = write(fd, buf, n);
   if (n  < 0) {
     err = IO_ERROR;
     goto exit;
   }
  }
  
  uint64_t next = stack_next_update_index(st);
   
  struct slice next_name = {};
  format_name(&next_name, next, next);

  slice_set_string(&temp_tab_name, st->reftable_dir);
  slice_append_string(&temp_tab_name, "/");
  slice_append(&temp_tab_name, next_name);
  slice_append_string(&temp_tab_name, "XXXXXX");
     
  int tab_fd = mkstemp((char*)slice_as_string(&temp_tab_name));
  if (tab_fd < 0 ) {
    err = IO_ERROR;
    goto exit;
  }

  struct writer *wr = new_writer( fd_writer, &tab_fd, &st->cfg);
  err = write_table(wr, arg);
  if (err < 0) {
    goto exit;
  }
   
  err = writer_close(wr);
  if (err < 0) {
    goto exit;
  }

  err = close(tab_fd);
  if (err < 0) {
    err = IO_ERROR;
    goto exit;
  }

  // TODO check min_update_index
  slice_append_string(&next_name, ".ref");

  struct slice tab_name;
  slice_set_string(&tab_name, st->reftable_dir);
  slice_append_string(&tab_name, "/");
  slice_append(&tab_name, next_name);
  slice_append_string(&tab_name, "");
   
  err = rename(slice_as_string(&temp_tab_name), slice_as_string(&tab_name));
  if (err < 0) {
    err = IO_ERROR;
    goto exit;
  }

  free(slice_yield(&temp_tab_name));
   
  {
    char buf[1024];
    int n = sprintf(buf, "%s\n", slice_as_string(&next_name));
    n = write(fd, buf, n);
    if (n  < 0) {
      err = IO_ERROR;
      goto exit;
    }
  }

  err = close(fd);
  if (err < 0) {
    unlink(slice_as_string(&tab_name));
    err = IO_ERROR;
    goto exit;
  }

  err = rename(slice_as_string(&lock_name), st->list_file);
  if (err < 0) {
    unlink(slice_as_string(&tab_name));
    err = IO_ERROR;
    goto exit;
  }
   
  err = stack_reload(st);
 exit:
  if (tab_fd > 0) {
    close(tab_fd);
    tab_fd = 0;
  }
  if (temp_tab_name.len > 0) {
    unlink(slice_as_string(&temp_tab_name));
  }
  unlink(slice_as_string(&lock_name));
  
  if (fd > 0) {
    close(fd);
    fd = 0;
  }

  free(slice_yield(&temp_tab_name));
  free(slice_yield(&tab_name));
  free(slice_yield(&next_name));
  return err;
}

uint64_t stack_next_update_index(struct stack* st) {
  int sz = st->merged->stack_len;
  if (sz > 0) {
    return reader_max_update_index( st->merged->stack[sz-1]) + 1;
  }
  return 1;
}
   
int stack_compact_locked(struct stack* st, int first, int last, struct slice* temp_tab) {
  struct slice next_name= {};
  format_name(&next_name, reader_min_update_index(st->merged->stack[first]),
	      reader_max_update_index(st->merged->stack[first]));

  slice_set_string(temp_tab, st->reftable_dir);
  slice_append_string(temp_tab, "/");
  slice_append(temp_tab, next_name);
  slice_append_string(temp_tab, "XXXXXX");
     
  int tab_fd = mkstemp((char*)slice_as_string(temp_tab));

  struct writer *wr = new_writer(fd_writer, &tab_fd, &st->cfg);

  int err = stack_write_compact(wr, first, last);
  if (err < 0) {
    goto exit;
  }
  err = writer_close(wr);
  if (err < 0) {
    goto exit;
  }

  err = close(tab_fd);
  tab_fd = 0;
  
 exit:
  if (tab_fd >0 ) {
    close(tab_fd);
    tab_fd = 0;
  }
  if (err != 0 && temp_tab->len > 0) {
    unlink(slice_as_string(temp_tab));
    free(slice_yield(temp_tab));
  }
  free(slice_yield(&next_name));
  return err;
}

int stack_write_compact(struct writer *wr, int first, int last) {
  assert(false);
  return 0;
}

