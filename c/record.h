#ifndef RECORD_H
#define RECORD_H

#include "slice.h"
#include "api.h"

int get_var_int(uint64 *dest, slice in);
int put_var_int(slice dest, uint64 val);
int common_prefix_size(slice a, slice b);

int is_block_type(byte typ);
int ref_record_encode(const record *rec, slice s);
int ref_record_decode(record *rec, slice key, byte val_type, slice in);
void ref_record_key(const record *r, slice *dest);
record* new_record(byte typ);

int encode_key(bool *restart, slice dest, slice prev_key, slice key, byte extra);
int decode_key(slice* key, byte *extra, slice last_key, slice in);

typedef struct {
  record_ops *ops;
  slice last_key;
  uint64 offset;
} index_record;

extern record_ops index_record_ops;

byte index_record_type();
void index_record_key(const record *r, slice *dest);
void index_record_copy_from(record* rec, const record *src_rec);
void index_record_free(record* rec);
byte index_record_val_type(const record *rec);
int index_record_encode(const record *rec, slice s);
int index_record_decode(record *rec, slice key, byte val_type, slice in) ;

typedef struct {
  record_ops *ops;
  byte *hash_prefix;
  int hash_prefix_len;
  uint64 *offsets;
  int offset_len;
} obj_record;


byte obj_record_type();
void obj_record_key(const record *r, slice *dest);
void obj_record_copy_from(record* rec, const record *src_rec);
void obj_record_free(record* rec);
byte obj_record_val_type(const record *rec);
int obj_record_encode(const record *rec, slice s);
int obj_record_decode(record *rec, slice key, byte val_type, slice in) ;

extern record_ops obj_record_ops;

#endif


