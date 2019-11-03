
#include "tree.h"


typedef struct _writer {
  int (*write)(void *, byte *, int);
  void *write_arg;
  int pending_padding;

  slice last_key;

  uint64 next;
  write_options opts;

  byte *block;
  block_writer *block_writer;
  block_writer block_writer_data;
  index_record *index;
  int index_len;
  int index_cap;

  // tree for use with tsearch
  tree_node *obj_index_tree;

  stats stats;
} writer;

int writer_flush_block(writer *w);

