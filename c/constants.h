
#define HASH_SIZE 20
#define VERSION 1
#define HEADER_SIZE 24
#define FOOTER_SIZE 68

#define BLOCK_TYPE_LOG 'g'
#define BLOCK_TYPE_INDEX 'i'
#define BLOCK_TYPE_REF 'r'
#define BLOCK_TYPE_OBJ 'o'

#define MAX_RESTARTS ((1 << 16) - 1)
