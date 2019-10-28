#ifndef TEST_FRAMEWORK_H
#define TEST_FRAMEWORK_H

#include <stdio.h>
#include <stdlib.h>

#ifdef NDEBUG
#undef NDEBUG
#endif

#include <assert.h>

#ifdef assert 
#undef assert 
#endif

#define assert(c) \
  if (!(c)) { \
    fflush(stderr);\
    fflush(stdout);\
    fprintf(stderr, "%s: %d: failed assertion %s", __FILE__, __LINE__, #c); \
    abort();\
}


typedef struct  {
  const char *name;
  void (*testfunc)();
} test_case;

test_case* new_test_case (const char *name, void (*testfunc)());
test_case* add_test_case (const char *name, void (*testfunc)());
void test_main();

#endif
