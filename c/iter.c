#include "api.h"

int empty_iterator_next(void* arg, record rec) {
  return 1;
}

void empty_iterator_close(void* arg) {
}

iterator_ops empty_ops = {
		      .next= &empty_iterator_next,
		      .close = &empty_iterator_close,
};

void iterator_set_empty(iterator *it) {
  it->iter_arg = NULL;
  it->ops = &empty_ops;
}

int iterator_next(iterator it, record rec) {
  return it.ops->next(it.iter_arg, rec);
}

void iterator_close(iterator it) {
  return it.ops->close(it.iter_arg);
}
