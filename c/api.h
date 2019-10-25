#ifndef API_H
#define API_H

#include <stdint.h>
#include <stdio.h> // debug

#define true 1
#define false 0
#define ARRAYSIZE(a) sizeof(a)/sizeof(a[0])

typedef uint64_t uint64;
typedef uint32_t uint32;
typedef uint16_t uint16;
typedef uint8_t byte;
typedef byte bool;
typedef int error;

typedef struct record_t record;

typedef struct {
  byte *buf;
  int len;
  int cap;
} slice;

typedef struct {
  uint64 (*size)(void *source);
  error (*read_block)(void* source, slice* dest, uint64 off, uint32 size);
  void (*close)(void *source);
} block_source_ops;

typedef struct {
  block_source_ops *ops;
} block_source;

typedef struct  {
  void (*key)(record *rec, slice *dest);
  byte (*type)(record *rec);
  void (*copy_from)(record *rec, record *src);
  byte (*val_type)(record *rec);
  int (*encode)(record* rec, slice *dest);
  int (*decode)(record *rec, slice *src);
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
  record record;
  char* ref_name;
  uint64 update_index;
  char* value;
  char* target_value;
  char* target;
} ref_record;

typedef struct {
  record record;
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
  record record;
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

#endif
