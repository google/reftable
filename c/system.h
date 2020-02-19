/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#ifndef SYSTEM_H
#define SYSTEM_H

#if REFTABLE_IN_GITCORE

#include "git-compat-util.h"
#include <zlib.h>

#else

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <zlib.h>

#include "compat.h"

#endif /* REFTABLE_IN_GITCORE */

typedef uint8_t byte;
typedef int bool;

int uncompress_return_consumed(Bytef *dest, uLongf *destLen,
			       const Bytef *source, uLong *sourceLen);
#define SHA1_SIZE 20
#define SHA256_SIZE 32

#endif
