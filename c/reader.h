

typedef struct {
  bool present;
  uint64 offset;
  uint64 index_offset;
} reader_offsets;

typedef struct {
  block_source source;

  uint64 size;
  uint32 block_size;
  uint64 min_update_index;
  uint64 max_update_index;
  int object_id_len;

  reader_offsets ref_offsets;
  reader_offsets obj_offsets;
  reader_offsets log_offsets;
} reader;

int init_reader(reader *r, block_source source);
int reader_seek(reader* r, iterator* it, record rec);
