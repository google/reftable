/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#ifndef REFTABLE_RECORD_H
#define REFTABLE_RECORD_H

#include <stdint.h>

/*
 Basic data types

 Reftables store the state of each ref in struct reftable_ref_record, and they
 store a sequence of reflog updates in struct reftable_log_record.
*/

/* reftable_ref_record holds a ref database entry target_value */
struct reftable_ref_record {
	char *refname; /* Name of the ref, malloced. */
	uint64_t update_index; /* Logical timestamp at which this value is
				  written */
	uint8_t *value; /* SHA1, or NULL. malloced. */
	uint8_t *target_value; /* peeled annotated tag, or NULL. malloced. */
	char *target; /* symref, or NULL. malloced. */
};

/* returns whether 'ref' represents a deletion */
int reftable_ref_record_is_deletion(const struct reftable_ref_record *ref);

/* prints a reftable_ref_record onto stdout */
void reftable_ref_record_print(struct reftable_ref_record *ref,
			       uint32_t hash_id);

/* frees and nulls all pointer values. */
void reftable_ref_record_release(struct reftable_ref_record *ref);

/* returns whether two reftable_ref_records are the same */
int reftable_ref_record_equal(struct reftable_ref_record *a,
			      struct reftable_ref_record *b, int hash_size);

/* reftable_log_record holds a reflog entry */
struct reftable_log_record {
	char *refname;
	uint64_t update_index; /* logical timestamp of a transactional update.
				*/
	uint8_t *new_hash;
	uint8_t *old_hash;
	char *name;
	char *email;
	uint64_t time;
	int16_t tz_offset;
	char *message;
};

/* returns whether 'ref' represents the deletion of a log record. */
int reftable_log_record_is_deletion(const struct reftable_log_record *log);

/* frees and nulls all pointer values. */
void reftable_log_record_release(struct reftable_log_record *log);

/* returns whether two records are equal. Useful for testing. */
int reftable_log_record_equal(struct reftable_log_record *a,
			      struct reftable_log_record *b, int hash_size);

/* dumps a reftable_log_record on stdout, for debugging/testing. */
void reftable_log_record_print(struct reftable_log_record *log,
			       uint32_t hash_id);

#endif
