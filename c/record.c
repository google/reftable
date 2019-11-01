#include <assert.h>
#include <string.h>
#include <stdlib.h>

#include "api.h"
#include "record.h"

int is_block_type(byte typ) {
  switch (typ) {
  case BLOCK_TYPE_REF:
  case BLOCK_TYPE_LOG:
  case BLOCK_TYPE_OBJ:
  case BLOCK_TYPE_INDEX:
    return true;
  }
  return false;
}

int get_var_int(uint64 *dest, slice in) {
  if (in.len == 0) {
    return -1;
  }

  int ptr = 0;
  uint64 val = in.buf[ptr] & 0x7f;

  while ( in.buf[ptr] & 0x80 ) {
    ptr++;
    if (ptr > in.len) {
      return -1;
    }
    val = (val + 1) << 7 | (uint64)(in.buf[ptr]&0x7f);
  }

  *dest = val;
  return ptr+1;
}

int put_var_int(slice dest, uint64 val) {
  byte buf[10];

  int i = 9;
  buf[i] = (byte)(val & 0x7f);
  i--;
  while (true) {
    val >>= 7;
    if (!val) {
      break;
    }
    val--;
    buf[i] = 0x80 | (byte)(val&0x7f);
    i--;
  }

  int n = sizeof(buf) - i - 1;
  if (dest.len < n) { return -1; }
  memcpy(dest.buf, &buf[i+1], n);
  return n;
}

int common_prefix_size(slice a, slice b)  {
  int p = 0;
  while (p < a.len && p < b.len) {
    if (a.buf[p] != b.buf[p]) {
	break;
      }
    p++;
  }

  return p;
}

int decode_string(slice *dest, slice in) {
  int start_len = in.len;
  uint64 tsize = 0;
  int n = get_var_int( &tsize, in);
  if (n <= 0) {
    return -1;
  }
  in.buf += n;
  in.len -= n;
  if (in.len < tsize){
    return -1;
  }

  slice_resize(dest, tsize+1);
  dest->buf[tsize] = 0;
  memcpy(dest->buf, in.buf, tsize);
  in.buf += tsize;
  in.len -= tsize;

  return start_len - in.len;
}

int encode_key(bool *restart, slice dest, slice prev_key, slice key, byte extra) {
  slice start = dest;
  int prefix_len = common_prefix_size(prev_key, key);
  *restart = (prefix_len == 0);

  int n = put_var_int(dest, (uint64)prefix_len);
  if (n < 0) { return -1; }
  dest.buf += n;
  dest.len -= n;
  
  uint64 suffix_len = key.len - prefix_len;
  n = put_var_int(dest, suffix_len << 3 | (uint64)extra);
  if (n < 0) {
    return -1;
  }
  dest.buf += n;
  dest.len -= n;

  if (dest.len < suffix_len ) {
    return -1;
  }
  memcpy(dest.buf, key.buf + prefix_len, suffix_len);
  dest.buf += suffix_len;
  dest.len -= suffix_len;
  
  return start.len - dest.len;
}

byte ref_record_type() {
  return BLOCK_TYPE_REF;
}

void ref_record_key(const record *r, slice *dest) {
  ref_record * rec = (ref_record*) r;
  slice_set_string(dest, rec->ref_name);
}

void ref_record_copy_from(record* rec, const record *src_rec) {
  assert(src_rec->ops->type() == BLOCK_TYPE_REF);
  assert(rec->ops->type() == BLOCK_TYPE_REF);

  ref_record *ref =(ref_record*) rec;
  ref_record *src =(ref_record*) src_rec;

  memset(ref, 0, sizeof(ref_record));
  if (src->ref_name != NULL) {
    ref->ref_name = strdup(src->ref_name);
  }
  
  if (src->target != NULL) {
    ref->target = strdup(src->target);
  }

  if (src->target_value != NULL) {
    ref->target_value = malloc(HASH_SIZE);
    memcpy(ref->target_value, src->target_value, HASH_SIZE);
  }

  if (src->value != NULL) {
    ref->value = malloc(HASH_SIZE);
    memcpy(ref->value, src->value, HASH_SIZE);
  }
}

void ref_record_free(record* rec) {
  ref_record *ref =(ref_record*) rec;
  free(ref->ref_name);
  free(ref->target);
  free(ref->target_value);
  free(ref->value);
  memset(ref, 0, sizeof(ref_record));
}

byte ref_record_val_type(const record *rec) {
  ref_record *r = (ref_record*) rec;
  if (r->value  != NULL) {
    if (r->target_value != NULL) {
      return 2;
    } else {
      return 1;
    }
  } else if (r->target != NULL ) {
    return 3;
  }
  return 0;
}

int ref_record_encode(const record *rec, slice s) {
  ref_record *r = (ref_record*)rec;
  slice start = s;
  int n = put_var_int(s, r->update_index);
  if (n < 0) { return -1; }
  s.buf += n;
  s.len -= n;

  if (r->value != NULL) {
    if (s.len < HASH_SIZE ) {return -1;}
    memcpy(s.buf, r->value, HASH_SIZE);
    s.buf += HASH_SIZE;
    s.len -= HASH_SIZE;
  }

  if (r->target_value != NULL) {
    if (s.len < HASH_SIZE ) {return -1;}
    memcpy(s.buf, r->target_value, HASH_SIZE);
    s.buf += HASH_SIZE;
    s.len -= HASH_SIZE;
  }

  if (r->target != NULL){
    int l = strlen(r->target);
    n = put_var_int(s, l);
    if (n<0 ) {return -1;}
    s.buf += n;
    s.len-=n;
    if (s.len < l) {
      return -1;
    }
    memcpy(s.buf, r->target, l);
    s.buf += l;
    s.len -= l;
  }

  return start.len - s.len;
}


int ref_record_decode(record *rec, slice key, byte val_type, slice in) {
  ref_record *r = (ref_record*)rec;

  slice start = in;
  int n = get_var_int(&r->update_index, in);
  if ( n < 0) { return n; }

  in.buf += n;
  in.len -= n;

  r->ref_name = realloc(r->ref_name, key.len+1);
  memcpy(r->ref_name, key.buf, key.len);
  r->ref_name[key.len] = 0;
  switch (val_type) {
  case 1:
  case 2:
    if (in.len < HASH_SIZE) {
      return -1;
    }

    r->value = malloc(HASH_SIZE);
    memcpy(r->value, in.buf, HASH_SIZE);
    in.buf += HASH_SIZE;
    in.len -= HASH_SIZE;
    if (val_type == 1) {
      break;
    }
    r->target_value = malloc(HASH_SIZE);
    memcpy(r->target_value, in.buf, HASH_SIZE);
    in.buf += HASH_SIZE;
    in.len -= HASH_SIZE;
    break;
  case 3:
    {
      slice dest = {};
      int n = decode_string(&dest, in);
      if (n < 0) {
	return -1;
      }
      in.buf += n;
      in.len -= n;
      
      r->target = slice_to_string(dest);
      free(slice_yield(&dest));
    }
    break;
  }

  return start.len - in.len;
}

int decode_key(slice* key, byte *extra, slice last_key, slice in) {
  int start_len = in.len;
  uint64 prefix_len = 0;
  int n = get_var_int(&prefix_len, in);
  if (n < 0) {
    return -1;
  }
  in.buf += n;
  in.len -= n;

  if (prefix_len > last_key.len) {
    return -1;
  }

  uint64 suffix_len = 0;
  n = get_var_int( &suffix_len, in);
  if (n <= 0) {
    return -1;
  }
  in.buf += n;
  in.len -= n;
  
  *extra = (byte)(suffix_len & 0xf);
  suffix_len >>= 3;

  if (in.len < suffix_len){
    return -1;
  }
  
  slice_resize(key, suffix_len + prefix_len);
  memcpy(key->buf, last_key.buf, prefix_len);

  memcpy(key->buf + prefix_len, in.buf, suffix_len);
  in.buf += suffix_len;
  in.len -= suffix_len;

  return start_len - in.len;
}

record_ops ref_record_ops =
  {
   .key = &ref_record_key,
   .type = &ref_record_type,
   .copy_from = &ref_record_copy_from,
   .val_type = &ref_record_val_type,
   .encode = &ref_record_encode,
   .decode = &ref_record_decode,
   .free = &ref_record_free,
  };



byte obj_record_type() {
  return BLOCK_TYPE_OBJ;
}

void obj_record_key(const record *r, slice *dest) {
  obj_record * rec = (obj_record*) r;
  slice_resize(dest, rec->hash_prefix_len);
  memcpy(dest->buf, rec->hash_prefix, rec->hash_prefix_len);
}

void obj_record_copy_from(record* rec, const record *src_rec) {
  assert(src_rec->ops->type() == BLOCK_TYPE_REF);
  assert(rec->ops->type() == BLOCK_TYPE_REF);

  obj_record *ref = (obj_record*) rec;
  obj_record *src = (obj_record*) src_rec;

  *ref = *src;
  ref->hash_prefix = malloc(ref->hash_prefix_len);
  memcpy(ref->hash_prefix, src->hash_prefix, ref->hash_prefix_len);

  int olen = ref->offset_len * sizeof(uint64);
  ref->offsets = malloc(olen);
  memcpy(ref->offsets, src->offsets, olen);
}  

void obj_record_free(record* rec) {
  obj_record *ref =(obj_record*) rec;
  free(ref->hash_prefix);
  free(ref->offsets);
  memset(ref, 0, sizeof(obj_record));
}

byte obj_record_val_type(const record *rec) {
  obj_record *r = (obj_record*) rec;
  if (r->offset_len > 0 &&r->offset_len < 8 ) {
    return r->offset_len;
  }
  return 0;
}

int obj_record_encode(const record *rec, slice s) {
  obj_record *r = (obj_record*)rec;
  slice start = s;
  if (r->offset_len == 0 || r->offset_len >= 8) {
    int n = put_var_int(s, r->offset_len);
    if (n < 0) { return -1; }
    s.buf += n;
    s.len -= n;
  }
  if (r->offset_len == 0) {
    return start.len - s.len;
  }
  int n = put_var_int(s, r->offsets[0]);
  if (n < 0) { return -1; }
  s.buf += n;
  s.len -= n;

  uint64 last = r->offsets[0];
  for (int i= 1; i < r->offset_len; i++) {
    int n = put_var_int(s, r->offsets[i] - last);
    if (n < 0) { return -1; }
    s.buf += n;
    s.len -= n;
    last = r->offsets[i];
  }
  
  return start.len - s.len;
}


int obj_record_decode(record *rec, slice key, byte val_type, slice in) {
  slice start = in;
  obj_record *r = (obj_record*)rec;
  
  r->hash_prefix = malloc(key.len);
  memcpy(r->hash_prefix, key.buf, key.len);
  r->hash_prefix_len = key.len;

  uint64 count;
  if (val_type == 0) {
    int n = get_var_int(&count, in);
    if (n < 0) {
      return n;
    }
    
    in.buf += n;
    in.len -= n;
  } else {
    count = val_type;
  }

  r->offsets = NULL;
  r->offset_len = 0;
  if (count == 0) {
    return start.len - in.len;
  }

  r->offsets = malloc(count  * sizeof(uint64));
  r->offset_len = count;

  int n = get_var_int(&r->offsets[0], in);
  if (n < 0) { return n; }
    
  in.buf += n;
  in.len -= n;

  uint64 last = r->offsets[0];

  int j = 1;
  while (j < count) {
    uint64 delta; 
    int n = get_var_int(&delta, in);
    if (n < 0) { return n; }
    
    in.buf += n;
    in.len -= n;
    
    last = r->offsets[j] = (delta  + last);
    j++;
  }
  return start.len - in.len;
}

record_ops obj_record_ops =
  {
   .key = &obj_record_key,
   .type = &obj_record_type,
   .copy_from = &obj_record_copy_from,
   .val_type = &obj_record_val_type,
   .encode = &obj_record_encode,
   .decode = &obj_record_decode,
   .free = &obj_record_free,
  };

record* new_record(byte typ) {
  switch (typ) {
  case BLOCK_TYPE_REF:
    {
      ref_record *r = calloc(1, sizeof(ref_record));
      r->ops = &ref_record_ops;
      return (record*) r;
    }
  }
  assert(0);  
}


byte index_record_type() {
  return BLOCK_TYPE_INDEX;
}

void index_record_key(const record *r, slice *dest) {
  index_record * rec = (index_record*) r;
  slice_copy(dest, rec->last_key);
}

void index_record_copy_from(record* rec, const record *src_rec) {
  assert(src_rec->ops->type() == BLOCK_TYPE_REF);
  assert(rec->ops->type() == BLOCK_TYPE_REF);

  index_record *dst = (index_record*) rec;
  index_record *src = (index_record*) src_rec;

  slice_copy(&dst->last_key, src->last_key);
  dst->offset = src->offset;
}

void index_record_free(record* rec) {
  index_record *idx =(index_record*) rec;
  free(slice_yield(&idx->last_key));
}

byte index_record_val_type(const record *rec) {
  return 0;
}

int index_record_encode(const record *rec, slice out) {
  index_record *r = (index_record*)rec;
  slice start = out;

  int n = put_var_int(out, r->offset);
  if (n <0 ) {
    return n;
  }

  out.buf += n;
  out.len -= n;
  
  return start.len - out.len;
}

int index_record_decode(record *rec, slice key, byte val_type, slice in) {
  slice start = in;
  index_record *r = (index_record*)rec;

  slice_copy(&r->last_key, key);
  
  int n = get_var_int(&r->offset, in);
  if (n < 0) {
    return n;
  }
    
  in.buf += n;
  in.len -= n;
  return start.len - in.len;
}

record_ops index_record_ops =
  {
   .key = &index_record_key,
   .type = &index_record_type,
   .copy_from = &index_record_copy_from,
   .val_type = &index_record_val_type,
   .encode = &index_record_encode,
   .decode = &index_record_decode,
   .free = &index_record_free,
  };
