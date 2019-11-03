#ifndef ITER_H
#define ITER_H

struct _iterator_ops {
  int (*next)(void *iter_arg, record rec);
  void (*close)(void *iter_arg);
};

void iterator_set_empty(iterator *it);
int iterator_next(iterator it, record rec);

#endif
