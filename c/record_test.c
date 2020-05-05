/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "record.h"

#include "system.h"
#include "basics.h"
#include "constants.h"
#include "reftable.h"
#include "test_framework.h"

void test_copy(struct record rec)
{
	struct record copy = new_record(record_type(rec));
	record_copy_from(copy, rec, SHA1_SIZE);
	switch (record_type(copy)) {
	case BLOCK_TYPE_REF:
		assert(reftable_ref_record_equal(
			record_as_ref(copy), record_as_ref(rec), SHA1_SIZE));
		break;
	case BLOCK_TYPE_LOG:
		assert(reftable_log_record_equal(
			record_as_log(copy), record_as_log(rec), SHA1_SIZE));
		break;
	}
	record_destroy(&copy);
}

void varint_roundtrip()
{
	uint64_t inputs[] = { 0,
			      1,
			      27,
			      127,
			      128,
			      257,
			      4096,
			      ((uint64_t)1 << 63),
			      ((uint64_t)1 << 63) + ((uint64_t)1 << 63) - 1 };
	int i = 0;
	for (i = 0; i < ARRAY_SIZE(inputs); i++) {
		byte dest[10];

		struct slice out = { .buf = dest, .len = 10, .cap = 10 };

		uint64_t in = inputs[i];
		int n = put_var_int(out, in);
		assert(n > 0);
		out.len = n;

		uint64_t got = 0;
		n = get_var_int(&got, out);
		assert(n > 0);

		assert(got == in);
	}
}

void test_common_prefix()
{
	struct {
		const char *a, *b;
		int want;
	} cases[] = {
		{ "abc", "ab", 2 },
		{ "", "abc", 0 },
		{ "abc", "abd", 2 },
		{ "abc", "pqr", 0 },
	};

	int i = 0;
	for (i = 0; i < ARRAY_SIZE(cases); i++) {
		struct slice a = { 0 };
		struct slice b = { 0 };
		slice_set_string(&a, cases[i].a);
		slice_set_string(&b, cases[i].b);

		int got = common_prefix_size(a, b);
		assert(got == cases[i].want);

		slice_clear(&a);
		slice_clear(&b);
	}
}

void set_hash(byte *h, int j)
{
	int i = 0;
	for (i = 0; i < hash_size(SHA1_ID); i++) {
		h[i] = (j >> i) & 0xff;
	}
}

void test_reftable_ref_record_roundtrip()
{
	int i = 0;
	for (i = 0; i <= 3; i++) {
		printf("subtest %d\n", i);
		struct reftable_ref_record in = { 0 };
		switch (i) {
		case 0:
			break;
		case 1:
			in.value = reftable_malloc(SHA1_SIZE);
			set_hash(in.value, 1);
			break;
		case 2:
			in.value = reftable_malloc(SHA1_SIZE);
			set_hash(in.value, 1);
			in.target_value = reftable_malloc(SHA1_SIZE);
			set_hash(in.target_value, 2);
			break;
		case 3:
			in.target = xstrdup("target");
			break;
		}
		in.ref_name = xstrdup("refs/heads/master");

		struct record rec = { 0 };
		record_from_ref(&rec, &in);
		test_copy(rec);

		assert(record_val_type(rec) == i);
		byte buf[1024];
		struct slice key = { 0 };
		record_key(rec, &key);
		struct slice dest = {
			.buf = buf,
			.len = sizeof(buf),
		};
		int n = record_encode(rec, dest, SHA1_SIZE);
		assert(n > 0);

		struct reftable_ref_record out = { 0 };
		struct record rec_out = { 0 };
		record_from_ref(&rec_out, &out);
		int m = record_decode(rec_out, key, i, dest, SHA1_SIZE);
		assert(n == m);

		assert((out.value != NULL) == (in.value != NULL));
		assert((out.target_value != NULL) == (in.target_value != NULL));
		assert((out.target != NULL) == (in.target != NULL));
		slice_clear(&key);
		record_clear(rec_out);
		reftable_ref_record_clear(&in);
	}
}

void test_reftable_log_record_equal()
{
	struct reftable_log_record in[2] = {
		{
			.ref_name = xstrdup("refs/heads/master"),
			.update_index = 42,
		},
		{
			.ref_name = xstrdup("refs/heads/master"),
			.update_index = 22,
		}
	};

	assert(!reftable_log_record_equal(&in[0], &in[1], SHA1_SIZE));
	in[1].update_index = in[0].update_index;
	assert(reftable_log_record_equal(&in[0], &in[1], SHA1_SIZE));
	reftable_log_record_clear(&in[0]);
	reftable_log_record_clear(&in[1]);
}

void test_reftable_log_record_roundtrip()
{
	struct reftable_log_record in[2] = {
		{
			.ref_name = xstrdup("refs/heads/master"),
			.old_hash = reftable_malloc(SHA1_SIZE),
			.new_hash = reftable_malloc(SHA1_SIZE),
			.name = xstrdup("han-wen"),
			.email = xstrdup("hanwen@google.com"),
			.message = xstrdup("test"),
			.update_index = 42,
			.time = 1577123507,
			.tz_offset = 100,
		},
		{
			.ref_name = xstrdup("refs/heads/master"),
			.update_index = 22,
		}
	};
	set_test_hash(in[0].new_hash, 1);
	set_test_hash(in[0].old_hash, 2);
	for (int i = 0; i < ARRAY_SIZE(in); i++) {
		struct record rec = { 0 };
		record_from_log(&rec, &in[i]);

		test_copy(rec);

		struct slice key = { 0 };
		record_key(rec, &key);

		byte buf[1024];
		struct slice dest = {
			.buf = buf,
			.len = sizeof(buf),
		};

		int n = record_encode(rec, dest, SHA1_SIZE);
		assert(n >= 0);

		struct reftable_log_record out = { 0 };
		struct record rec_out = { 0 };
		record_from_log(&rec_out, &out);
		int valtype = record_val_type(rec);
		int m = record_decode(rec_out, key, valtype, dest, SHA1_SIZE);
		assert(n == m);

		assert(reftable_log_record_equal(&in[i], &out, SHA1_SIZE));
		reftable_log_record_clear(&in[i]);
		slice_clear(&key);
		record_clear(rec_out);
	}
}
void test_u24_roundtrip()
{
	uint32_t in = 0x112233;
	byte dest[3];

	put_be24(dest, in);
	uint32_t out = get_be24(dest);
	assert(in == out);
}

void test_key_roundtrip()
{
	struct slice dest = { 0 }, last_key = { 0 }, key = { 0 },
		     roundtrip = { 0 };

	slice_resize(&dest, 1024);
	slice_set_string(&last_key, "refs/heads/master");
	slice_set_string(&key, "refs/tags/bla");

	bool restart;
	byte extra = 6;
	int n = encode_key(&restart, dest, last_key, key, extra);
	assert(!restart);
	assert(n > 0);

	byte rt_extra;
	int m = decode_key(&roundtrip, &rt_extra, last_key, dest);
	assert(n == m);
	assert(slice_equal(key, roundtrip));
	assert(rt_extra == extra);

	slice_clear(&last_key);
	slice_clear(&key);
	slice_clear(&dest);
	slice_clear(&roundtrip);
}

void print_bytes(byte *p, int l)
{
	int i = 0;
	for (i = 0; i < l; i++) {
		byte c = *p;
		if (c < 32) {
			c = '.';
		}
		printf("%02x[%c] ", p[i], c);
	}
	printf("(%d)\n", l);
}

void test_obj_record_roundtrip()
{
	byte testHash1[SHA1_SIZE] = { 0 };
	set_hash(testHash1, 1);
	uint64_t till9[] = { 1, 2, 3, 4, 500, 600, 700, 800, 9000 };

	struct obj_record recs[3] = { {
					      .hash_prefix = testHash1,
					      .hash_prefix_len = 5,
					      .offsets = till9,
					      .offset_len = 3,
				      },
				      {
					      .hash_prefix = testHash1,
					      .hash_prefix_len = 5,
					      .offsets = till9,
					      .offset_len = 9,
				      },
				      {
					      .hash_prefix = testHash1,
					      .hash_prefix_len = 5,
				      }

	};
	int i = 0;
	for (i = 0; i < ARRAY_SIZE(recs); i++) {
		printf("subtest %d\n", i);
		struct obj_record in = recs[i];
		byte buf[1024];
		struct record rec = { 0 };
		record_from_obj(&rec, &in);

		test_copy(rec);

		struct slice key = { 0 };
		record_key(rec, &key);
		struct slice dest = {
			.buf = buf,
			.len = sizeof(buf),
		};
		int n = record_encode(rec, dest, SHA1_SIZE);
		assert(n > 0);
		byte extra = record_val_type(rec);
		struct obj_record out = { 0 };
		struct record rec_out = { 0 };
		record_from_obj(&rec_out, &out);
		int m = record_decode(rec_out, key, extra, dest, SHA1_SIZE);
		assert(n == m);

		assert(in.hash_prefix_len == out.hash_prefix_len);
		assert(in.offset_len == out.offset_len);

		assert(!memcmp(in.hash_prefix, out.hash_prefix,
			       in.hash_prefix_len));
		assert(0 == memcmp(in.offsets, out.offsets,
				   sizeof(uint64_t) * in.offset_len));
		slice_clear(&key);
		record_clear(rec_out);
	}
}

void test_index_record_roundtrip()
{
	struct index_record in = { .offset = 42 };

	slice_set_string(&in.last_key, "refs/heads/master");

	struct slice key = { 0 };
	struct record rec = { 0 };
	record_from_index(&rec, &in);
	record_key(rec, &key);
	test_copy(rec);

	assert(0 == slice_compare(key, in.last_key));

	byte buf[1024];
	struct slice dest = {
		.buf = buf,
		.len = sizeof(buf),
	};
	int n = record_encode(rec, dest, SHA1_SIZE);
	assert(n > 0);

	byte extra = record_val_type(rec);
	struct index_record out = { 0 };
	struct record out_rec = { NULL };
	record_from_index(&out_rec, &out);
	int m = record_decode(out_rec, key, extra, dest, SHA1_SIZE);
	assert(m == n);

	assert(in.offset == out.offset);

	record_clear(out_rec);
	slice_clear(&key);
	slice_clear(&in.last_key);
}

int main(int argc, char *argv[])
{
	add_test_case("test_reftable_log_record_equal",
		      &test_reftable_log_record_equal);
	add_test_case("test_reftable_log_record_roundtrip",
		      &test_reftable_log_record_roundtrip);
	add_test_case("test_reftable_ref_record_roundtrip",
		      &test_reftable_ref_record_roundtrip);
	add_test_case("varint_roundtrip", &varint_roundtrip);
	add_test_case("test_key_roundtrip", &test_key_roundtrip);
	add_test_case("test_common_prefix", &test_common_prefix);
	add_test_case("test_obj_record_roundtrip", &test_obj_record_roundtrip);
	add_test_case("test_index_record_roundtrip",
		      &test_index_record_roundtrip);
	add_test_case("test_u24_roundtrip", &test_u24_roundtrip);
	test_main(argc, argv);
}
