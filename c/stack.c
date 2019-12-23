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

#include "api.h"
#include "stack.h"
#include "merged.h"
#include "reader.h"
#include "writer.h"

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
    stack_destroy(p);
  } else {
    *dest = p;
  }
  return err;
}



int read_lines(const char *filename, char ***namesp) {
  FILE *f = fopen(filename, "r");
  if (f == NULL) {
    int e = errno;
    if (e == ENOENT) {
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
    return IO_ERROR;
  }
  buf[size] = 0;
  
  parse_names(buf, size, namesp);

 exit:
  free(buf);
  fclose(f);
  return err;
}

struct merged_table *stack_merged_table(struct stack *st) {
  return st->merged;
}

/* Close and free the stack */
void stack_destroy(struct stack *st) {
  if (st->merged == NULL) {
    return;
  }

  merged_table_close(st->merged);
  merged_table_free(st->merged);
  st->merged = NULL;

  free(st->list_file);
  st->list_file = NULL;
  free(st->reftable_dir);
  st->reftable_dir = NULL;
  free(st);
}

int stack_reload_once(struct stack* st, char **names) {
  int cur_len = st->merged == NULL ? 0 : st->merged->stack_len;
  struct reader **cur = calloc(sizeof(struct reader*), cur_len);
  for (int i = 0; i < cur_len; i++) {
    cur[i] = st->merged->stack[i];
  }

  int err = 0;
  int names_len = 0;
  for (char **p = names; *p; p++) {
    names_len ++;
  }

  struct reader **new_tables = malloc(sizeof(struct reader*)*names_len);
  int new_tables_len = 0;

  struct slice table_path = {};

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
      slice_set_string(&table_path, st->reftable_dir);
      slice_append_string(&table_path, "/");
      slice_append_string(&table_path, name);

      struct block_source src = {};
      err = block_source_from_file(&src, slice_as_string(&table_path));
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

  new_tables = NULL;
  new_tables_len = 0;
  if (st->merged != NULL) {
    merged_table_clear(st->merged);
    merged_table_free(st->merged);
  }
  st->merged = new_merged;
  
 exit:
  free(slice_yield(&table_path));
  for (int i = 0; i < new_tables_len; i++) {
    reader_close(new_tables[i]);
  }
  free(new_tables);
  free(cur);
  return err;
}

// XXX const?
struct merged_table *stack_merged(struct stack* st) {
  return st->merged;
}

// return negative if a before b.
int tv_cmp(struct timeval *a, struct timeval *b) {
  time_t diff =  a->tv_sec - b->tv_sec;
  if (diff != 0) {
      return diff;
  }
    
  suseconds_t udiff = a->tv_usec - b->tv_usec;
  return udiff;
}

int stack_reload(struct stack* st) {
  struct timeval deadline = {};
  int err = gettimeofday(&deadline, NULL);
  if (err < 0) {
    return err;
  }

  deadline.tv_sec += 3;
  int64_t delay = 0;
  int tries = 0; 
  while (true) {
    struct timeval now = {};
    int err = gettimeofday(&now, NULL);
    if (err < 0) {
      return err;
    }

    // Only look at deadlines after the first few times. This
    // simplifies debugging in GDB
    tries ++;
    if (tries > 3 && tv_cmp(&now, &deadline) >= 0) {
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
    if (err != NOT_EXIST_ERROR) {
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

    delay = delay + (delay * rand()) / RAND_MAX + 100;
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
    if (err == LOCK_ERROR) {
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
  struct slice temp_tab_name = {};
  struct slice tab_name = {};
  struct slice next_name = {};
  struct slice lock_suffix = {};
  struct writer *wr = NULL;
  
  slice_set_string(&lock_name, st->list_file);
  slice_set_string(&lock_suffix, ".lock");
  slice_append(&lock_name, lock_suffix);

  int tab_fd = 0;
  int err = 0;
  int fd = 0;
  fd = open(slice_as_string(&lock_name), O_EXCL|O_CREAT|O_WRONLY, 0644);
  if (fd < 0) {
    if (errno == EEXIST) {
      err = LOCK_ERROR;
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
    err = LOCK_ERROR;
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
  
  uint64_t next_update_index = stack_next_update_index(st);

  slice_resize(&next_name, 0);
  format_name(&next_name, next_update_index, next_update_index);

  slice_set_string(&temp_tab_name, st->reftable_dir);
  slice_append_string(&temp_tab_name, "/");
  slice_append(&temp_tab_name, next_name);
  slice_append_string(&temp_tab_name, "XXXXXX");

  tab_fd = mkstemp((char*)slice_as_string(&temp_tab_name));
  if (tab_fd < 0 ) {
    err = IO_ERROR;
    goto exit;
  }


  wr = new_writer( fd_writer, &tab_fd, &st->cfg);
  err = write_table(wr, arg);
  if (err < 0) {
    goto exit;
  }
   
  err = writer_close(wr);
  if (err < 0) {
    goto exit;
  }
  
  err = close(tab_fd);
  tab_fd = 0;
  if (err < 0) {
    err = IO_ERROR;
    goto exit;
  }

  // TODO check min_update_index
  slice_append_string(&next_name, ".ref");

  slice_set_string(&tab_name, st->reftable_dir);
  slice_append_string(&tab_name, "/");

  if (wr->min_update_index < next_update_index) {
    err = API_ERROR;
    goto exit;
  }

  slice_resize(&next_name, 0);
  format_name(&next_name, wr->min_update_index, wr->max_update_index);
  slice_append_string(&next_name, ".ref");
  slice_append(&tab_name, next_name);
   
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
  fd = 0;
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

  free(slice_yield(&lock_suffix));
  free(slice_yield(&lock_name));
  free(slice_yield(&temp_tab_name));
  free(slice_yield(&tab_name));
  free(slice_yield(&next_name));
  writer_free(wr);
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

  int err = stack_write_compact(st, wr, first, last);
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

int stack_write_compact(struct stack *st, struct writer *wr, int first, int last) {
  writer_set_limits(wr,
		    st->merged->stack[first]->min_update_index,
		    st->merged->stack[last]->max_update_index);

  int subtabs_len = last - first + 1;
  struct reader** subtabs = calloc(sizeof(struct reader*), last - first + 1);
  for (int i = first, j = 0; i <= last; i++) {
    subtabs[j++] = st->merged->stack[i];
  }

  struct merged_table *mt = NULL;
  int err = new_merged_table(&mt, subtabs, subtabs_len);
  if (err < 0) {
    free(subtabs);
    goto exit;
  }

  struct iterator it = {};
  err = merged_table_seek_ref(mt, &it, "");
  if (err < 0) {
    goto exit;
  }

  struct ref_record ref = {};
  while (true) {
    err = iterator_next_ref(it, &ref);
    if (err > 0) {
      err = 0;
      break;
    }
    if (err < 0) {
      goto exit;
    }
    if (first == 0 && ref_record_is_deletion(&ref)) {
      continue;
    }
    
    err = writer_add_ref(wr, &ref);
    if (err < 0) {
      goto exit;
    }
  }
 exit:
  if (mt != NULL) {
    merged_table_clear(mt);
    merged_table_free(mt);
  }
  ref_record_clear(&ref);
  
  return err;
}


// <  0: error. 0 == OK,  > 0 attempt failed; could retry.
int stack_compact_range(struct stack *st, int first, int last) {
  if (first >= last) {
    return 0;
  }

  struct slice temp_tab_name = {};
  struct slice new_table_name = {};
  struct slice lock_file_name = {};

  st->stats.attempts++;
  int err = 0;

  // XXX in function?
  slice_set_string(&lock_file_name, st->list_file);
  slice_append_string(&lock_file_name, ".lock");

  int have_lock = false;
  int lock_file_fd = open(slice_as_string(&lock_file_name), O_EXCL| O_CREAT|O_WRONLY, 0644);
  if (lock_file_fd < 0){
    if (errno == EEXIST) {
      err = 1;
    } else {
      err = IO_ERROR;
    }
    goto exit;
  }
  have_lock = true;
  err = stack_uptodate(st);
  if (err != 0) {
    goto exit;
  }

  int compact_count = last - first + 1;
  char **delete_on_success = calloc(sizeof(char*), compact_count+1);
  char **subtable_locks = calloc(sizeof(char*), compact_count+1);

  for (int i = first, j = 0; i <= last; i++) {
    struct slice subtab_name = {};
    struct slice subtab_lock = {};
    slice_set_string(&subtab_name, st->reftable_dir);
    slice_append_string(&subtab_name, "/");
    slice_append_string(&subtab_name, reader_name(st->merged->stack[i]));

    slice_copy(&subtab_lock, subtab_name);
    slice_append_string(&subtab_lock, ".lock");

    int lock_file_fd = open(slice_as_string(&subtab_lock), O_EXCL| O_CREAT|O_WRONLY, 0644);
    if (lock_file_fd < 0) {
      // XXX mem leak for slices
      if (errno == EEXIST) {
	err = 1;
	goto exit;
      }
      err = IO_ERROR;
      goto exit;
    }

    close(lock_file_fd);
    subtable_locks[j] = (char*)slice_yield(&subtab_lock);
    delete_on_success[j] = (char*)slice_yield(&subtab_name);
    j++;
  }

  err = unlink(slice_as_string(&lock_file_name));
  if (err < 0) {
    goto exit;
  }
  have_lock = false;

  err = stack_compact_locked(st, first ,last, &temp_tab_name);
  if (err < 0) {
    goto exit;
  }

  // XXX in function?
  lock_file_fd = open(slice_as_string(&lock_file_name), O_EXCL| O_CREAT|O_WRONLY, 0644);
  if (lock_file_fd < 0){
    if (errno == EEXIST) {
      err = 1;
    } else {
      err = IO_ERROR;
    }
    goto exit;
  }
  have_lock = true;

  format_name(&new_table_name,
	      st->merged->stack[first]->min_update_index,
	      st->merged->stack[last]->max_update_index);
  slice_append_string(&new_table_name, ".ref");

  struct slice new_table_path={};
  slice_set_string(&new_table_path, st->reftable_dir);
  slice_append_string(&new_table_path, "/");

  slice_append(&new_table_path, new_table_name);

  err = rename(slice_as_string(&temp_tab_name),
	       slice_as_string(&new_table_path));
  if (err < 0) {
    goto exit;
  }

  struct slice ref_list_contents = {};
  for (int i =0; i < first; i++) {
    slice_append_string(&ref_list_contents, st->merged->stack[i]->name);
    slice_append_string(&ref_list_contents, "\n");
  }
  slice_append(&ref_list_contents, new_table_name);
  slice_append_string(&ref_list_contents, "\n");
  for (int i = last+1; i < st->merged->stack_len; i++) {
    slice_append_string(&ref_list_contents, st->merged->stack[i]->name);
    slice_append_string(&ref_list_contents, "\n");
  }

  err = write(lock_file_fd, ref_list_contents.buf, ref_list_contents.len);
  if (err < 0){
    unlink(slice_as_string(&new_table_path));
    goto exit;
  }
  err = close(lock_file_fd);
  lock_file_fd = 0;
  if (err < 0) {
    unlink(slice_as_string(&new_table_path));
    goto exit;
  }

  err = rename(slice_as_string(&lock_file_name), st->list_file);
  if (err < 0) {
    unlink(slice_as_string(&new_table_path));
    goto exit;
  }
  have_lock = false;

  for (char **p = delete_on_success; *p; p++) {
    unlink(*p);
  }

  err = stack_reload(st);
 exit:
  
  free(slice_yield(&temp_tab_name));
  for (char **p = subtable_locks; *p; p++) {
    unlink(*p);
  }
  free_names(delete_on_success);
  free_names(subtable_locks);
  if (lock_file_fd > 0) {
    close(lock_file_fd);
    lock_file_fd = 0;
  }
  if (have_lock) {
    unlink(slice_as_string(&lock_file_name));
  }
  free(slice_yield(&lock_file_name));
  return err;
}

int stack_compact_all(struct stack* st) {
  return stack_compact_range(st, 0, st->merged->stack_len-1);
}
