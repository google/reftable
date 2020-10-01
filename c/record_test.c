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
#include "reftable-tests.h"

static void test_copy(struct reftable_record *rec)
{
	struct reftable_record copy =
		reftable_new_record(reftable_record_type(rec));
	reftable_record_copy_from(&copy, rec, SHA1_SIZE);
	/* do it twice to catch memory leaks */
	reftable_record_copy_from(&copy, rec, SHA1_SIZE);
	switch (reftable_record_type(&copy)) {
	case BLOCK_TYPE_REF:
		assert(reftable_ref_record_equal(reftable_record_as_ref(&copy),
						 reftable_record_as_ref(rec),
						 SHA1_SIZE));
		break;
	case BLOCK_TYPE_LOG:
		assert(reftable_log_record_equal(reftable_record_as_log(&copy),
						 reftable_record_as_log(rec),
						 SHA1_SIZE));
		break;
	}
	reftable_record_destroy(&copy);
}

static void test_varint_roundtrip(void)
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
		uint8_t dest[10];

		struct string_view out = {
			.buf = dest,
			.len = sizeof(dest),
		};
		uint64_t in = inputs[i];
		int n = put_var_int(&out, in);
		uint64_t got = 0;

		assert(n > 0);
		out.len = n;
		n = get_var_int(&got, &out);
		assert(n > 0);

		assert(got == in);
	}
}

static void test_common_prefix(void)
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
		struct strbuf a = STRBUF_INIT;
		struct strbuf b = STRBUF_INIT;
		strbuf_addstr(&a, cases[i].a);
		strbuf_addstr(&b, cases[i].b);
		assert(common_prefix_size(&a, &b) == cases[i].want);

		strbuf_release(&a);
		strbuf_release(&b);
	}
}

static void set_hash(uint8_t *h, int j)
{
	int i = 0;
	for (i = 0; i < hash_size(SHA1_ID); i++) {
		h[i] = (j >> i) & 0xff;
	}
}

static void test_reftable_ref_record_roundtrip(void)
{
	int i = 0;

	for (i = 0; i <= 3; i++) {
		struct reftable_ref_record in = { NULL };
		struct reftable_ref_record out = {
			.refname = xstrdup("old name"),
			.value = reftable_calloc(SHA1_SIZE),
			.target_value = reftable_calloc(SHA1_SIZE),
			.target = xstrdup("old value"),
		};
		struct reftable_record rec_out = { NULL };
		struct strbuf key = STRBUF_INIT;
		struct reftable_record rec = { NULL };
		uint8_t buffer[1024] = { 0 };
		struct string_view dest = {
			.buf = buffer,
			.len = sizeof(buffer),
		};

		int n, m;

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
		in.refname = xstrdup("refs/heads/master");

		reftable_record_from_ref(&rec, &in);
		test_copy(&rec);

		assert(reftable_record_val_type(&rec) == i);

		reftable_record_key(&rec, &key);
		n = reftable_record_encode(&rec, dest, SHA1_SIZE);
		assert(n > 0);

		/* decode into a non-zero reftable_record to test for leaks. */

		reftable_record_from_ref(&rec_out, &out);
		m = reftable_record_decode(&rec_out, key, i, dest, SHA1_SIZE);
		assert(n == m);

		assert((out.value != NULL) == (in.value != NULL));
		assert((out.target_value != NULL) == (in.target_value != NULL));
		assert((out.target != NULL) == (in.target != NULL));
		reftable_record_clear(&rec_out);

		strbuf_release(&key);
		reftable_ref_record_clear(&in);
	}
}

static void test_reftable_log_record_equal(void)
{
	struct reftable_log_record in[2] = {
		{
			.refname = xstrdup("refs/heads/master"),
			.update_index = 42,
		},
		{
			.refname = xstrdup("refs/heads/master"),
			.update_index = 22,
		}
	};

	assert(!reftable_log_record_equal(&in[0], &in[1], SHA1_SIZE));
	in[1].update_index = in[0].update_index;
	assert(reftable_log_record_equal(&in[0], &in[1], SHA1_SIZE));
	reftable_log_record_clear(&in[0]);
	reftable_log_record_clear(&in[1]);
}

static void test_reftable_log_record_roundtrip(void)
{
	struct reftable_log_record in[2] = {
		{
			.refname = xstrdup("refs/heads/master"),
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
			.refname = xstrdup("refs/heads/master"),
			.update_index = 22,
		}
	};
	set_test_hash(in[0].new_hash, 1);
	set_test_hash(in[0].old_hash, 2);
	for (int i = 0; i < ARRAY_SIZE(in); i++) {
		struct reftable_record rec = { NULL };
		struct strbuf key = STRBUF_INIT;
		uint8_t buffer[1024] = { 0 };
		struct string_view dest = {
			.buf = buffer,
			.len = sizeof(buffer),
		};
		/* populate out, to check for leaks. */
		struct reftable_log_record out = {
			.refname = xstrdup("old name"),
			.new_hash = reftable_calloc(SHA1_SIZE),
			.old_hash = reftable_calloc(SHA1_SIZE),
			.name = xstrdup("old name"),
			.email = xstrdup("old@email"),
			.message = xstrdup("old message"),
		};
		struct reftable_record rec_out = { NULL };
		int n, m, valtype;

		reftable_record_from_log(&rec, &in[i]);

		test_copy(&rec);

		reftable_record_key(&rec, &key);

		n = reftable_record_encode(&rec, dest, SHA1_SIZE);
		assert(n >= 0);
		reftable_record_from_log(&rec_out, &out);
		valtype = reftable_record_val_type(&rec);
		m = reftable_record_decode(&rec_out, key, valtype, dest,
					   SHA1_SIZE);
		assert(n == m);

		assert(reftable_log_record_equal(&in[i], &out, SHA1_SIZE));
		reftable_log_record_clear(&in[i]);
		strbuf_release(&key);
		reftable_record_clear(&rec_out);
	}
}

static void test_u24_roundtrip(void)
{
	uint32_t in = 0x112233;
	uint8_t dest[3];
	uint32_t out;
	put_be24(dest, in);
	out = get_be24(dest);
	assert(in == out);
}

static void test_key_roundtrip(void)
{
	uint8_t buffer[1024] = { 0 };
	struct string_view dest = {
		.buf = buffer,
		.len = sizeof(buffer),
	};
	struct strbuf last_key = STRBUF_INIT;
	struct strbuf key = STRBUF_INIT;
	struct strbuf roundtrip = STRBUF_INIT;
	int restart;
	uint8_t extra;
	int n, m;
	uint8_t rt_extra;

	strbuf_addstr(&last_key, "refs/heads/master");
	strbuf_addstr(&key, "refs/tags/bla");
	extra = 6;
	n = reftable_encode_key(&restart, dest, last_key, key, extra);
	assert(!restart);
	assert(n > 0);

	m = reftable_decode_key(&roundtrip, &rt_extra, last_key, dest);
	assert(n == m);
	assert(0 == strbuf_cmp(&key, &roundtrip));
	assert(rt_extra == extra);

	strbuf_release(&last_key);
	strbuf_release(&key);
	strbuf_release(&roundtrip);
}

static void test_reftable_obj_record_roundtrip(void)
{
	uint8_t testHash1[SHA1_SIZE] = { 1, 2, 3, 4, 0 };
	uint64_t till9[] = { 1, 2, 3, 4, 500, 600, 700, 800, 9000 };
	struct reftable_obj_record recs[3] = { {
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
					       } };
	int i = 0;
	for (i = 0; i < ARRAY_SIZE(recs); i++) {
		struct reftable_obj_record in = recs[i];
		uint8_t buffer[1024] = { 0 };
		struct string_view dest = {
			.buf = buffer,
			.len = sizeof(buffer),
		};
		struct reftable_record rec = { NULL };
		struct strbuf key = STRBUF_INIT;
		struct reftable_obj_record out = { NULL };
		struct reftable_record rec_out = { NULL };
		int n, m;
		uint8_t extra;

		reftable_record_from_obj(&rec, &in);
		test_copy(&rec);
		reftable_record_key(&rec, &key);
		n = reftable_record_encode(&rec, dest, SHA1_SIZE);
		assert(n > 0);
		extra = reftable_record_val_type(&rec);
		reftable_record_from_obj(&rec_out, &out);
		m = reftable_record_decode(&rec_out, key, extra, dest,
					   SHA1_SIZE);
		assert(n == m);

		assert(in.hash_prefix_len == out.hash_prefix_len);
		assert(in.offset_len == out.offset_len);

		assert(!memcmp(in.hash_prefix, out.hash_prefix,
			       in.hash_prefix_len));
		assert(0 == memcmp(in.offsets, out.offsets,
				   sizeof(uint64_t) * in.offset_len));
		strbuf_release(&key);
		reftable_record_clear(&rec_out);
	}
}

static void test_reftable_index_record_roundtrip(void)
{
	struct reftable_index_record in = {
		.offset = 42,
		.last_key = STRBUF_INIT,
	};
	uint8_t buffer[1024] = { 0 };
	struct string_view dest = {
		.buf = buffer,
		.len = sizeof(buffer),
	};
	struct strbuf key = STRBUF_INIT;
	struct reftable_record rec = { NULL };
	struct reftable_index_record out = { .last_key = STRBUF_INIT };
	struct reftable_record out_rec = { NULL };
	int n, m;
	uint8_t extra;

	strbuf_addstr(&in.last_key, "refs/heads/master");
	reftable_record_from_index(&rec, &in);
	reftable_record_key(&rec, &key);
	test_copy(&rec);

	assert(0 == strbuf_cmp(&key, &in.last_key));
	n = reftable_record_encode(&rec, dest, SHA1_SIZE);
	assert(n > 0);

	extra = reftable_record_val_type(&rec);
	reftable_record_from_index(&out_rec, &out);
	m = reftable_record_decode(&out_rec, key, extra, dest, SHA1_SIZE);
	assert(m == n);

	assert(in.offset == out.offset);

	reftable_record_clear(&out_rec);
	strbuf_release(&key);
	strbuf_release(&in.last_key);
}

int record_test_main(int argc, const char *argv[])
{
	add_test_case("test_reftable_log_record_equal",
		      &test_reftable_log_record_equal);
	add_test_case("test_reftable_log_record_roundtrip",
		      &test_reftable_log_record_roundtrip);
	add_test_case("test_reftable_ref_record_roundtrip",
		      &test_reftable_ref_record_roundtrip);
	add_test_case("test_varint_roundtrip", &test_varint_roundtrip);
	add_test_case("test_key_roundtrip", &test_key_roundtrip);
	add_test_case("test_common_prefix", &test_common_prefix);
	add_test_case("test_reftable_obj_record_roundtrip",
		      &test_reftable_obj_record_roundtrip);
	add_test_case("test_reftable_index_record_roundtrip",
		      &test_reftable_index_record_roundtrip);
	add_test_case("test_u24_roundtrip", &test_u24_roundtrip);
	return test_main(argc, argv);
}
