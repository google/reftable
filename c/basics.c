/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "basics.h"

#include "system.h"

void put_be24(byte *out, uint32_t i)
{
	out[0] = (byte)((i >> 16) & 0xff);
	out[1] = (byte)((i >> 8) & 0xff);
	out[2] = (byte)(i & 0xff);
}

uint32_t get_be24(byte *in)
{
	return (uint32_t)(in[0]) << 16 | (uint32_t)(in[1]) << 8 |
	       (uint32_t)(in[2]);
}

void put_be16(uint8_t *out, uint16_t i)
{
	out[0] = (uint8_t)((i >> 8) & 0xff);
	out[1] = (uint8_t)(i & 0xff);
}

int binsearch(size_t sz, int (*f)(size_t k, void *args), void *args)
{
	size_t lo = 0;
	size_t hi = sz;

	/* invariant: (hi == sz) || f(hi) == true
	   (lo == 0 && f(0) == true) || fi(lo) == false
	 */
	while (hi - lo > 1) {
		size_t mid = lo + (hi - lo) / 2;

		int val = f(mid, args);
		if (val) {
			hi = mid;
		} else {
			lo = mid;
		}
	}

	if (lo == 0) {
		if (f(0, args)) {
			return 0;
		} else {
			return 1;
		}
	}

	return hi;
}

void free_names(char **a)
{
	char **p = a;
	if (p == NULL) {
		return;
	}
	while (*p) {
		reftable_free(*p);
		p++;
	}
	reftable_free(a);
}

int names_length(char **names)
{
	int len = 0;
	char **p = names;
	while (*p) {
		p++;
		len++;
	}
	return len;
}

void parse_names(char *buf, int size, char ***namesp)
{
	char **names = NULL;
	int names_cap = 0;
	int names_len = 0;

	char *p = buf;
	char *end = buf + size;
	while (p < end) {
		char *next = strchr(p, '\n');
		if (next != NULL) {
			*next = 0;
		} else {
			next = end;
		}
		if (p < next) {
			if (names_len == names_cap) {
				names_cap = 2 * names_cap + 1;
				names = reftable_realloc(
					names, names_cap * sizeof(char *));
			}
			names[names_len++] = xstrdup(p);
		}
		p = next + 1;
	}

	if (names_len == names_cap) {
		names_cap = 2 * names_cap + 1;
		names = reftable_realloc(names, names_cap * sizeof(char *));
	}

	names[names_len] = NULL;
	*namesp = names;
}

int names_equal(char **a, char **b)
{
	while (*a && *b) {
		if (strcmp(*a, *b)) {
			return 0;
		}

		a++;
		b++;
	}

	return *a == *b;
}

const char *reftable_error_str(int err)
{
	static char buf[250];
	switch (err) {
	case REFTABLE_IO_ERROR:
		return "I/O error";
	case REFTABLE_FORMAT_ERROR:
		return "corrupt reftable file";
	case REFTABLE_NOT_EXIST_ERROR:
		return "file does not exist";
	case REFTABLE_LOCK_ERROR:
		return "data is outdated";
	case REFTABLE_API_ERROR:
		return "misuse of the reftable API";
	case REFTABLE_ZLIB_ERROR:
		return "zlib failure";
	case REFTABLE_NAME_CONFLICT:
		return "file/directory conflict";
	case REFTABLE_REFNAME_ERROR:
		return "invalid refname";
	case -1:
		return "general error";
	default:
		snprintf(buf, sizeof(buf), "unknown error code %d", err);
		return buf;
	}
}

int reftable_error_to_errno(int err)
{
	switch (err) {
	case REFTABLE_IO_ERROR:
		return EIO;
	case REFTABLE_FORMAT_ERROR:
		return EFAULT;
	case REFTABLE_NOT_EXIST_ERROR:
		return ENOENT;
	case REFTABLE_LOCK_ERROR:
		return EBUSY;
	case REFTABLE_API_ERROR:
		return EINVAL;
	case REFTABLE_ZLIB_ERROR:
		return EDOM;
	default:
		return ERANGE;
	}
}

void *(*reftable_malloc_ptr)(size_t sz) = &malloc;
void *(*reftable_realloc_ptr)(void *, size_t) = &realloc;
void (*reftable_free_ptr)(void *) = &free;

void *reftable_malloc(size_t sz)
{
	return (*reftable_malloc_ptr)(sz);
}

void *reftable_realloc(void *p, size_t sz)
{
	return (*reftable_realloc_ptr)(p, sz);
}

void reftable_free(void *p)
{
	reftable_free_ptr(p);
}

void *reftable_calloc(size_t sz)
{
	void *p = reftable_malloc(sz);
	memset(p, 0, sz);
	return p;
}

void reftable_set_alloc(void *(*malloc)(size_t),
			void *(*realloc)(void *, size_t), void (*free)(void *))
{
	reftable_malloc_ptr = malloc;
	reftable_realloc_ptr = realloc;
	reftable_free_ptr = free;
}
