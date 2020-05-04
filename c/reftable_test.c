/*
Copyright 2020 Google LLC

Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file or at
https://developers.google.com/open-source/licenses/bsd
*/

#include "reftable.h"

#include "system.h"

#include "basics.h"
#include "block.h"
#include "constants.h"
#include "reader.h"
#include "record.h"
#include "test_framework.h"

static const int update_index = 5;

void test_buffer(void)
{
	struct slice buf = { 0 };

	byte in[] = "hello";
	slice_write(&buf, in, sizeof(in));
	struct reftable_block_source source;
	block_source_from_slice(&source, &buf);
	assert(block_source_size(source) == 6);
	struct reftable_block out = { 0 };
	int n = block_source_read_block(source, &out, 0, sizeof(in));
	assert(n == sizeof(in));
	assert(!memcmp(in, out.data, n));
	block_source_return_block(source, &out);

	n = block_source_read_block(source, &out, 1, 2);
	assert(n == 2);
	assert(!memcmp(out.data, "el", 2));

	block_source_return_block(source, &out);
	block_source_close(&source);
	slice_clear(&buf);
}

void test_default_write_opts(void)
{
	struct reftable_write_options opts = { 0 };
	struct slice buf = { 0 };
	struct reftable_writer *w =
		reftable_new_writer(&slice_write_void, &buf, &opts);
	reftable_writer_set_limits(w, 1, 1);

	struct reftable_ref_record rec = {
		.ref_name = "master",
		.update_index = 1,
	};

	int err = reftable_writer_add_ref(w, &rec);
	assert_err(err);

	err = reftable_writer_close(w);
	assert_err(err);

	struct reftable_block_source source = { 0 };
	block_source_from_slice(&source, &buf);

	struct reftable_reader *rd = NULL;
	err = reftable_new_reader(&rd, source, "filename");
	assert_err(err);

	uint32_t hash_id = reftable_reader_hash_id(rd);
	assert(hash_id == SHA1_ID);

	struct reftable_reader **readers = malloc(sizeof(*readers) * 1);
	readers[0] = rd;

	struct reftable_merged_table *merged = NULL;
	err = reftable_new_merged_table(&merged, readers, 1, SHA1_ID);
	assert_err(err);

	reftable_merged_table_close(merged);
	reftable_merged_table_free(merged);
}

void write_table(char ***names, struct slice *buf, int N, int block_size,
		 uint32_t hash_id)
{
	*names = reftable_calloc(sizeof(char *) * (N + 1));

	struct reftable_write_options opts = {
		.block_size = block_size,
		.hash_id = hash_id,
	};

	struct reftable_writer *w =
		reftable_new_writer(&slice_write_void, buf, &opts);

	reftable_writer_set_limits(w, update_index, update_index);
	{
		struct reftable_ref_record ref = { 0 };
		int i = 0;
		for (i = 0; i < N; i++) {
			byte hash[SHA256_SIZE] = { 0 };
			set_test_hash(hash, i);

			char name[100];
			snprintf(name, sizeof(name), "refs/heads/branch%02d",
				 i);

			ref.ref_name = name;
			ref.value = hash;
			ref.update_index = update_index;
			(*names)[i] = xstrdup(name);

			int n = reftable_writer_add_ref(w, &ref);
			assert(n == 0);
		}
	}
	{
		struct reftable_log_record log = { 0 };
		int i = 0;
		for (i = 0; i < N; i++) {
			byte hash[SHA256_SIZE] = { 0 };
			set_test_hash(hash, i);

			char name[100];
			snprintf(name, sizeof(name), "refs/heads/branch%02d",
				 i);

			log.ref_name = name;
			log.new_hash = hash;
			log.update_index = update_index;
			log.message = "message";

			int n = reftable_writer_add_log(w, &log);
			assert(n == 0);
		}
	}

	int n = reftable_writer_close(w);
	assert(n == 0);

	const struct reftable_stats *stats = writer_stats(w);
	int i = 0;
	for (i = 0; i < stats->ref_stats.blocks; i++) {
		int off = i * opts.block_size;
		if (off == 0) {
			off = header_size((hash_id == SHA256_ID) ? 2 : 1);
		}
		assert(buf->buf[off] == 'r');
	}

	assert(stats->log_stats.blocks > 0);
	reftable_writer_free(w);
}

void test_log_buffer_size(void)
{
	struct slice buf = { 0 };
	struct reftable_write_options opts = {
		.block_size = 4096,
	};

	struct reftable_writer *w =
		reftable_new_writer(&slice_write_void, &buf, &opts);

	reftable_writer_set_limits(w, update_index, update_index);

	/* This tests buffer extension for log compression. Must use a random
	   hash, to ensure that the compressed part is larger than the original.
	*/
	byte hash1[SHA1_SIZE], hash2[SHA1_SIZE];
	for (int i = 0; i < SHA1_SIZE; i++) {
		hash1[i] = (byte)(rand() % 256);
		hash2[i] = (byte)(rand() % 256);
	}
	struct reftable_log_record log = {
		.ref_name = "refs/heads/master",
		.old_hash = hash1,
		.new_hash = hash2,
		.name = "Han-Wen Nienhuys",
		.email = "hanwen@google.com",
		.tz_offset = 100,
		.time = 0x5e430672,
		.update_index = 0xa,
		.message = "commit: 9\n",
	};
	int err = reftable_writer_add_log(w, &log);
	assert_err(err);
	err = reftable_writer_close(w);
	assert_err(err);
}

void test_log_write_read(void)
{
	int N = 2;
	char **names = reftable_calloc(sizeof(char *) * (N + 1));

	struct reftable_write_options opts = {
		.block_size = 256,
	};

	struct slice buf = { 0 };
	struct reftable_writer *w =
		reftable_new_writer(&slice_write_void, &buf, &opts);

	reftable_writer_set_limits(w, 0, N);
	{
		struct reftable_ref_record ref = { 0 };
		int i = 0;
		for (i = 0; i < N; i++) {
			char name[256];
			snprintf(name, sizeof(name), "b%02d%0*d", i, 130, 7);
			names[i] = xstrdup(name);
			puts(name);
			ref.ref_name = name;
			ref.update_index = i;

			int err = reftable_writer_add_ref(w, &ref);
			assert_err(err);
		}
	}

	{
		struct reftable_log_record log = { 0 };
		int i = 0;
		for (i = 0; i < N; i++) {
			byte hash1[SHA1_SIZE], hash2[SHA1_SIZE];
			set_test_hash(hash1, i);
			set_test_hash(hash2, i + 1);

			log.ref_name = names[i];
			log.update_index = i;
			log.old_hash = hash1;
			log.new_hash = hash2;

			int err = reftable_writer_add_log(w, &log);
			assert_err(err);
		}
	}

	int n = reftable_writer_close(w);
	assert(n == 0);

	const struct reftable_stats *stats = writer_stats(w);
	assert(stats->log_stats.blocks > 0);
	reftable_writer_free(w);
	w = NULL;

	struct reftable_block_source source = { 0 };
	block_source_from_slice(&source, &buf);

	struct reftable_reader rd = { 0 };
	int err = init_reader(&rd, source, "file.log");
	assert_err(err);

	{
		struct reftable_iterator it = { 0 };
		err = reftable_reader_seek_ref(&rd, &it, names[N - 1]);
		assert(err == 0);

		struct reftable_ref_record ref = { 0 };
		err = reftable_iterator_next_ref(it, &ref);
		assert_err(err);

		/* end of iteration. */
		err = reftable_iterator_next_ref(it, &ref);
		assert(0 < err);

		reftable_iterator_destroy(&it);
		reftable_ref_record_clear(&ref);
	}

	{
		struct reftable_iterator it = { 0 };
		err = reftable_reader_seek_log(&rd, &it, "");
		assert(err == 0);

		struct reftable_log_record log = { 0 };
		int i = 0;
		while (true) {
			int err = reftable_iterator_next_log(it, &log);
			if (err > 0) {
				break;
			}

			assert_err(err);
			assert_streq(names[i], log.ref_name);
			assert(i == log.update_index);
			i++;
		}

		assert(i == N);
		reftable_iterator_destroy(&it);
	}

	/* cleanup. */
	slice_clear(&buf);
	free_names(names);
	reader_close(&rd);
}

void test_table_read_write_sequential(void)
{
	char **names;
	struct slice buf = { 0 };
	int N = 50;
	write_table(&names, &buf, N, 256, SHA1_ID);

	struct reftable_block_source source = { 0 };
	block_source_from_slice(&source, &buf);

	struct reftable_reader rd = { 0 };
	int err = init_reader(&rd, source, "file.ref");
	assert(err == 0);

	struct reftable_iterator it = { 0 };
	err = reftable_reader_seek_ref(&rd, &it, "");
	assert(err == 0);

	int j = 0;
	while (true) {
		struct reftable_ref_record ref = { 0 };
		int r = reftable_iterator_next_ref(it, &ref);
		assert(r >= 0);
		if (r > 0) {
			break;
		}
		assert(0 == strcmp(names[j], ref.ref_name));
		assert(update_index == ref.update_index);

		j++;
		reftable_ref_record_clear(&ref);
	}
	assert(j == N);
	reftable_iterator_destroy(&it);
	slice_clear(&buf);
	free_names(names);

	reader_close(&rd);
}

void test_table_write_small_table(void)
{
	char **names;
	struct slice buf = { 0 };
	int N = 1;
	write_table(&names, &buf, N, 4096, SHA1_ID);
	assert(buf.len < 200);
	slice_clear(&buf);
	free_names(names);
}

void test_table_read_api(void)
{
	char **names;
	struct slice buf = { 0 };
	int N = 50;
	write_table(&names, &buf, N, 256, SHA1_ID);

	struct reftable_reader rd = { 0 };
	struct reftable_block_source source = { 0 };
	block_source_from_slice(&source, &buf);

	int err = init_reader(&rd, source, "file.ref");
	assert(err == 0);

	struct reftable_iterator it = { 0 };
	err = reftable_reader_seek_ref(&rd, &it, names[0]);
	assert(err == 0);

	struct reftable_log_record log = { 0 };
	err = reftable_iterator_next_log(it, &log);
	assert(err == REFTABLE_API_ERROR);

	slice_clear(&buf);
	int i = 0;
	for (i = 0; i < N; i++) {
		reftable_free(names[i]);
	}
	reftable_free(names);
	reader_close(&rd);
}

void test_table_read_write_seek(bool index, int hash_id)
{
	char **names;
	struct slice buf = { 0 };
	int N = 50;
	write_table(&names, &buf, N, 256, hash_id);

	struct reftable_reader rd = { 0 };
	struct reftable_block_source source = { 0 };
	block_source_from_slice(&source, &buf);

	int err = init_reader(&rd, source, "file.ref");
	assert(err == 0);
	assert(hash_id == reftable_reader_hash_id(&rd));

	if (!index) {
		rd.ref_offsets.index_offset = 0;
	} else {
		assert(rd.ref_offsets.index_offset > 0);
	}

	int i = 0;
	for (i = 1; i < N; i++) {
		struct reftable_iterator it = { 0 };
		int err = reftable_reader_seek_ref(&rd, &it, names[i]);
		assert(err == 0);
		struct reftable_ref_record ref = { 0 };
		err = reftable_iterator_next_ref(it, &ref);
		assert(err == 0);
		assert(0 == strcmp(names[i], ref.ref_name));
		assert(i == ref.value[0]);

		reftable_ref_record_clear(&ref);
		reftable_iterator_destroy(&it);
	}

	{
		struct reftable_iterator it = { 0 };
		struct slice pastLast = { 0 };
		slice_set_string(&pastLast, names[N - 1]);
		slice_append_string(&pastLast, "/");

		int err = reftable_reader_seek_ref(&rd, &it,
						   slice_as_string(&pastLast));
		assert(err > 0);
	}

	slice_clear(&buf);
	for (i = 0; i < N; i++) {
		reftable_free(names[i]);
	}
	reftable_free(names);
	reader_close(&rd);
}

void test_table_read_write_seek_linear(void)
{
	test_table_read_write_seek(false, SHA1_ID);
}

void test_table_read_write_seek_linear_sha256(void)
{
	test_table_read_write_seek(false, SHA256_ID);
}

void test_table_read_write_seek_index(void)
{
	test_table_read_write_seek(true, SHA1_ID);
}

void test_table_refs_for(bool indexed)
{
	int N = 50;
	char **want_names = reftable_calloc(sizeof(char *) * (N + 1));
	int want_names_len = 0;
	byte want_hash[SHA1_SIZE];
	set_test_hash(want_hash, 4);

	struct reftable_write_options opts = {
		.block_size = 256,
	};

	struct slice buf = { 0 };
	struct reftable_writer *w =
		reftable_new_writer(&slice_write_void, &buf, &opts);
	{
		struct reftable_ref_record ref = { 0 };
		int i = 0;
		for (i = 0; i < N; i++) {
			byte hash[SHA1_SIZE];
			memset(hash, i, sizeof(hash));
			char fill[51] = { 0 };
			memset(fill, 'x', 50);
			char name[100];
			/* Put the variable part in the start */
			snprintf(name, sizeof(name), "br%02d%s", i, fill);
			name[40] = 0;
			ref.ref_name = name;

			byte hash1[SHA1_SIZE];
			byte hash2[SHA1_SIZE];

			set_test_hash(hash1, i / 4);
			set_test_hash(hash2, 3 + i / 4);
			ref.value = hash1;
			ref.target_value = hash2;

			/* 80 bytes / entry, so 3 entries per block. Yields 17
			 */
			/* blocks. */
			int n = reftable_writer_add_ref(w, &ref);
			assert(n == 0);

			if (!memcmp(hash1, want_hash, SHA1_SIZE) ||
			    !memcmp(hash2, want_hash, SHA1_SIZE)) {
				want_names[want_names_len++] = xstrdup(name);
			}
		}
	}

	int n = reftable_writer_close(w);
	assert(n == 0);

	reftable_writer_free(w);
	w = NULL;

	struct reftable_reader rd;
	struct reftable_block_source source = { 0 };
	block_source_from_slice(&source, &buf);

	int err = init_reader(&rd, source, "file.ref");
	assert(err == 0);
	if (!indexed) {
		rd.obj_offsets.present = 0;
	}

	struct reftable_iterator it = { 0 };
	err = reftable_reader_seek_ref(&rd, &it, "");
	assert(err == 0);
	reftable_iterator_destroy(&it);

	err = reftable_reader_refs_for(&rd, &it, want_hash, SHA1_SIZE);
	assert(err == 0);

	struct reftable_ref_record ref = { 0 };

	int j = 0;
	while (true) {
		int err = reftable_iterator_next_ref(it, &ref);
		assert(err >= 0);
		if (err > 0) {
			break;
		}

		assert(j < want_names_len);
		assert(0 == strcmp(ref.ref_name, want_names[j]));
		j++;
		reftable_ref_record_clear(&ref);
	}
	assert(j == want_names_len);

	slice_clear(&buf);
	free_names(want_names);
	reftable_iterator_destroy(&it);
	reader_close(&rd);
}

void test_table_refs_for_no_index(void)
{
	test_table_refs_for(false);
}

void test_table_refs_for_obj_index(void)
{
	test_table_refs_for(true);
}

void test_table_empty(void)
{
	struct reftable_write_options opts = { 0 };
	struct slice buf = { 0 };
	struct reftable_writer *w =
		reftable_new_writer(&slice_write_void, &buf, &opts);
	reftable_writer_set_limits(w, 1, 1);

	int err = reftable_writer_close(w);
	assert(err == REFTABLE_EMPTY_TABLE_ERROR);
	reftable_writer_free(w);

	assert(buf.len == header_size(1) + footer_size(1));

	struct reftable_block_source source = { 0 };
	block_source_from_slice(&source, &buf);

	struct reftable_reader *rd = NULL;
	err = reftable_new_reader(&rd, source, "filename");
	assert_err(err);

	struct reftable_iterator it = { 0 };
	err = reftable_reader_seek_ref(rd, &it, "");
	assert_err(err);

	struct reftable_ref_record rec = { 0 };
	err = reftable_iterator_next_ref(it, &rec);
	assert(err > 0);

	reftable_iterator_destroy(&it);
	reftable_reader_free(rd);
	slice_clear(&buf);
}

int main(int argc, char *argv[])
{
	add_test_case("test_default_write_opts", test_default_write_opts);
	add_test_case("test_log_write_read", test_log_write_read);
	add_test_case("test_table_read_write_seek_linear_sha256",
		      &test_table_read_write_seek_linear_sha256);
	add_test_case("test_log_buffer_size", test_log_buffer_size);
	add_test_case("test_table_write_small_table",
		      &test_table_write_small_table);
	add_test_case("test_buffer", &test_buffer);
	add_test_case("test_table_read_api", &test_table_read_api);
	add_test_case("test_table_read_write_sequential",
		      &test_table_read_write_sequential);
	add_test_case("test_table_read_write_seek_linear",
		      &test_table_read_write_seek_linear);
	add_test_case("test_table_read_write_seek_index",
		      &test_table_read_write_seek_index);
	add_test_case("test_table_read_write_refs_for_no_index",
		      &test_table_refs_for_no_index);
	add_test_case("test_table_read_write_refs_for_obj_index",
		      &test_table_refs_for_obj_index);
	add_test_case("test_table_empty", &test_table_empty);
	test_main(argc, argv);
}
