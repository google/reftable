/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#ifndef SYSTEM_H
#define SYSTEM_H

#include "git-compat-util.h"

#ifdef REFTABLE_STANDALONE
struct strbuf;
int remove_dir_recursively(struct strbuf *path, int flags);
#endif

#define SHA1_ID 0x73686131
#define SHA256_ID 0x73323536
#define SHA1_SIZE 20
#define SHA256_SIZE 32

/* This is uncompress2, which is only available in zlib as of 2017.
 */
int uncompress_return_consumed(Bytef *dest, uLongf *destLen,
			       const Bytef *source, uLong *sourceLen);
int hash_size(uint32_t id);

#endif
