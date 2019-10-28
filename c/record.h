#ifndef RECORD_H
#define RECORD_H

#include "slice.h"
#include "api.h"

int get_var_int(uint64 *dest, slice in);
int put_var_int(slice dest, uint64 val);
int common_prefix_size(slice a, slice b);

int is_block_type(byte typ);
int ref_record_encode(const record *rec, slice s);
int ref_record_decode(record *rec, slice s, byte val_type);
record* new_record(byte typ);

int encode_key(bool *restart, slice dest, slice prev_key, slice key, byte extra);
int decode_key(slice* key, byte *extra, slice last_key, slice in);

#endif


