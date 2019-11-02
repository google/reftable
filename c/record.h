#ifndef RECORD_H
#define RECORD_H

#include "slice.h"
#include "api.h"

typedef struct _record_ops  {
  void (*key)(const void *rec, slice *dest);
  byte (*type)();
  void (*copy_from)(void *rec, const void *src);
  byte (*val_type)(const void *rec);
  int (*encode)(const void *rec, slice dest); 
  int (*decode)(void *rec, slice key, byte extra, slice src);
  void (*clear)(void *rec);
} record_ops;

int get_var_int(uint64 *dest, slice in);
int put_var_int(slice dest, uint64 val);
int common_prefix_size(slice a, slice b);

int is_block_type(byte typ);
record new_record(byte typ);

extern record_ops ref_record_ops;

int encode_key(bool *restart, slice dest, slice prev_key, slice key, byte extra);
int decode_key(slice* key, byte *extra, slice last_key, slice in);

typedef struct {
  slice last_key;
  uint64 offset;
} index_record;

typedef struct {
  byte *hash_prefix;
  int hash_prefix_len;
  uint64 *offsets;
  int offset_len;
} obj_record;

void record_key(record rec, slice *dest);
byte record_type(record rec);
void record_copy_from(record rec, record src);
byte record_val_type(record rec);
int record_encode(record rec, slice dest); 
int record_decode(record rec, slice key, byte extra, slice src);
void record_clear(record rec);
void *record_yield(record *rec);
void record_from_obj(record*rec, obj_record *objrec);
void record_from_index(record*rec, index_record *idxrec);
bool record_is_start(record want);

#endif


