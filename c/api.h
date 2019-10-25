typedef long int uint64;
typedef int uint32;
typedef short uint16;
typedef unsigned char byte;
typedef int error;

typedef struct {
  char *p;
  int len;
  int cap;
} slice;

typedef struct {
  uint64 (*size)(void *source);
  error (*read_block)(void* source, slice* dest, uint64 off, size uint32)
  (*close)(void *source);
} block_source;


typedef struct  {
  record_ops *ops;
} record;
   
typedef struct  {
  void (*key)(record *rec, slice *dest);
  byte (*type)(record *rec);
  void (*copy_from)(record *rec, record *src);
  byte (*val_type)(record *rec);
  int (*encode)(record* rec, slice *dest);
  int (*decode)(record *rec, slice *src);
} record_ops;

typedef struct {
  bool unpadded ;
  uint32 block_size;
  uint32 min_update_index;
  uint32 max_update_index;
  bool index_objects;
  restart_interval int;
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
  int (*next)(*record);
} iterator_ops;

typedef struct {
  iterator_ops ops;
} iterator;

