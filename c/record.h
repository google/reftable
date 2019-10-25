#ifndef RECORD_H
#define RECORD_H

int get_var_int(uint64 *dest, slice in);
int put_var_int(slice dest, uint64 val);

#endif
