#ifndef API_H
#define API_H

#include <stdint.h>
#include <stdio.h> // debug

#include "basics.h"
#include "slice.h"
#include "constants.h"

typedef struct record_t record;

typedef struct {
  uint64 (*size)(void *source);
  error (*read_block)(void* source, slice* dest, uint64 off, uint32 size);
  void (*close)(void *source);
} block_source_ops;

typedef struct {
  block_source_ops *ops;
} block_source;

typedef struct  {
  void (*key)(const record *rec, slice *dest);
  byte (*type)();
  void (*copy_from)(record *rec, const record *src);
  byte (*val_type)(const record *rec);
  int (*encode)(const record *rec, slice dest); 
  int (*decode)(record *rec, slice key, byte extra, slice src);
  void (*free)(record *rec);
} record_ops;

typedef struct record_t {
  record_ops *ops;
} record;
   
typedef struct {
  bool unpadded ;
  uint32 block_size;
  uint32 min_update_index;
  uint32 max_update_index;
  bool index_objects;
  int restart_interval;
} write_options;

typedef struct {
  record_ops *ops;
  char* ref_name;
  uint64 update_index;
  byte* value;
  byte* target_value;
  char* target;
} ref_record;

typedef struct {
  record_ops *ops;
  char *ref_name;
  uint64 update_index;
  char *new_hash;
  char *old_hash;
  char *name;
  char *email;
  uint64 time;
  uint64 tz_offset;
  char *message;
} log_record;

typedef struct {
  slice last_key;
  uint64 offset;
} index_record;

typedef struct {
  record_ops *ops;
  char *hash_prefix;
  uint64 *offsets;
  int offsets_len;
} obj_record;

typedef struct {
  int (*next)(record *rec);
} iterator_ops;

typedef struct {
  iterator_ops ops;
} iterator;

typedef struct {
  int entries;
  int restarts;
  int blocks;
  int index_blocks;
  int max_index_level;

  uint64 offset;
  uint64 index_offset;
} block_stats;

typedef struct {
  int blocks;
  block_stats ref_stats;
  block_stats obj_stats;
  block_stats idx_stats;
  // todo: log stats.
  int object_id_len;
} stats;

#endif
