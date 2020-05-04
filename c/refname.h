/*
  Copyright 2020 Google LLC

  Use of this source code is governed by a BSD-style
  license that can be found in the LICENSE file or at
  https://developers.google.com/open-source/licenses/bsd
*/
#ifndef REFNAME_H
#define REFNAME_H

#include "reftable.h"

struct modification {
	struct reftable_table tab;

	char **add;
	size_t add_len;

	char **del;
	size_t del_len;
};

// -1 = error, 0 = found, 1 = not found
int modification_has_ref(struct modification *mod, const char *name);

// -1 = error, 0 = found, 1 = not found.
int modification_has_ref_with_prefix(struct modification *mod,
				     const char *prefix);

// 0 = OK.
int validate_ref_name(const char *name);

int validate_ref_record_addition(struct reftable_table tab,
				 struct reftable_ref_record *recs, size_t sz);

int modification_validate(struct modification *mod);

#endif
