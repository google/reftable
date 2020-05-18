/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#ifndef RECORD_H
#define RECORD_H

#include "reftable.h"
#include "slice.h"

/* utilities for de/encoding varints */

int get_var_int(uint64_t *dest, struct slice in);
int put_var_int(struct slice dest, uint64_t val);

/* Methods for records. */
struct reftable_record_vtable {
	/* encode the key of to a byte slice. */
	void (*key)(const void *rec, struct slice *dest);

	/* The record type of ('r' for ref). */
	byte type;

	void (*copy_from)(void *dest, const void *src, int hash_size);

	/* a value of [0..7], indicating record subvariants (eg. ref vs. symref
	 * vs ref deletion) */
	byte (*val_type)(const void *rec);

	/* encodes rec into dest, returning how much space was used. */
	int (*encode)(const void *rec, struct slice dest, int hash_size);

	/* decode data from `src` into the record. */
	int (*decode)(void *rec, struct slice key, byte extra, struct slice src,
		      int hash_size);

	/* deallocate and null the record. */
	void (*clear)(void *rec);

	/* is this a tombstone? */
	bool (*is_deletion)(const void *rec);
};

/* record is a generic wrapper for different types of records. */
struct reftable_record {
	void *data;
	struct reftable_record_vtable *ops;
};

/* returns true for recognized block types. Block start with the block type. */
int reftable_is_block_type(byte typ);

/* creates a malloced record of the given type. Dispose with record_destroy */
struct reftable_record reftable_new_record(byte typ);

extern struct reftable_record_vtable reftable_ref_record_vtable;

/* Encode `key` into `dest`. Sets `restart` to indicate a restart. Returns
   number of bytes written. */
int reftable_encode_key(bool *restart, struct slice dest, struct slice prev_key,
			struct slice key, byte extra);

/* Decode into `key` and `extra` from `in` */
int reftable_decode_key(struct slice *key, byte *extra, struct slice last_key,
			struct slice in);

/* reftable_index_record are used internally to speed up lookups. */
struct reftable_index_record {
	uint64_t offset; /* Offset of block */
	struct slice last_key; /* Last key of the block. */
};

/* reftable_obj_record stores an object ID => ref mapping. */
struct reftable_obj_record {
	byte *hash_prefix; /* leading bytes of the object ID */
	int hash_prefix_len; /* number of leading bytes. Constant
			      * across a single table. */
	uint64_t *offsets; /* a vector of file offsets. */
	int offset_len;
};

/* see struct record_vtable */

void reftable_record_key(struct reftable_record *rec, struct slice *dest);
byte reftable_record_type(struct reftable_record *rec);
void reftable_record_copy_from(struct reftable_record *rec,
			       struct reftable_record *src, int hash_size);
byte reftable_record_val_type(struct reftable_record *rec);
int reftable_record_encode(struct reftable_record *rec, struct slice dest,
			   int hash_size);
int reftable_record_decode(struct reftable_record *rec, struct slice key,
			   byte extra, struct slice src, int hash_size);
bool reftable_record_is_deletion(struct reftable_record *rec);

/* zeroes out the embedded record */
void reftable_record_clear(struct reftable_record *rec);

/* clear out the record, yielding the reftable_record data that was
 * encapsulated. */
void *reftable_record_yield(struct reftable_record *rec);

/* clear and deallocate embedded record, and zero `rec`. */
void reftable_record_destroy(struct reftable_record *rec);

/* initialize generic records from concrete records. The generic record should
 * be zeroed out. */
void reftable_record_from_obj(struct reftable_record *rec,
			      struct reftable_obj_record *objrec);
void reftable_record_from_index(struct reftable_record *rec,
				struct reftable_index_record *idxrec);
void reftable_record_from_ref(struct reftable_record *rec,
			      struct reftable_ref_record *refrec);
void reftable_record_from_log(struct reftable_record *rec,
			      struct reftable_log_record *logrec);
struct reftable_ref_record *reftable_record_as_ref(struct reftable_record *ref);
struct reftable_log_record *reftable_record_as_log(struct reftable_record *ref);

/* for qsort. */
int reftable_ref_record_compare_name(const void *a, const void *b);

/* for qsort. */
int reftable_log_record_compare_key(const void *a, const void *b);

#endif
