/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#ifndef TEST_FRAMEWORK_H
#define TEST_FRAMEWORK_H

#include "system.h"

#ifdef NDEBUG
#undef NDEBUG
#endif

#ifdef assert
#undef assert
#endif

#define assert_err(c)                                                  \
	if (c != 0) {                                                  \
		fflush(stderr);                                        \
		fflush(stdout);                                        \
		fprintf(stderr, "%s: %d: error == %d (%s), want 0\n",  \
			__FILE__, __LINE__, c, reftable_error_str(c)); \
		abort();                                               \
	}

#define assert_streq(a, b)                                               \
	if (strcmp(a, b)) {                                              \
		fflush(stderr);                                          \
		fflush(stdout);                                          \
		fprintf(stderr, "%s:%d: %s (%s) != %s (%s)\n", __FILE__, \
			__LINE__, #a, a, #b, b);                         \
		abort();                                                 \
	}

#define assert(c)                                                          \
	if (!(c)) {                                                        \
		fflush(stderr);                                            \
		fflush(stdout);                                            \
		fprintf(stderr, "%s: %d: failed assertion %s\n", __FILE__, \
			__LINE__, #c);                                     \
		abort();                                                   \
	}

struct test_case {
	const char *name;
	void (*testfunc)(void);
};

struct test_case *new_test_case(const char *name, void (*testfunc)(void));
struct test_case *add_test_case(const char *name, void (*testfunc)(void));
int test_main(int argc, const char *argv[]);

void set_test_hash(uint8_t *p, int i);

#endif
