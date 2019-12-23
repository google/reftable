
This is a from scratch implementation of the reftable format for
storing Git ref and reflog data. 

The spec is at 
https://eclipse.googlesource.com/jgit/jgit/+/refs/heads/master/Documentation/technical/reftable.md

Based on the implementation, there is also [a proposal for a v2 of the
format](reftable-v2-proposal.md).


# GO

The Go implementation implements the spec completely


# C

An experimental implementation in C is under the directory c/ . It is
functionally equivalent to the Go version, except it

# Open questions

How to deal with HASH_SIZE != 20 ? Will repos exist in dual config, if
so what is the plan for ref transactions?

# Integration notes

In core git, the backend is initialized in

     find_ref_storage_backend

this looks for a backend by name. The struct to implement is
refs/refs-internal.h

     629:struct ref_storage_be {
     630: 	struct ref_storage_be *next;
     631: 	const char *name;
     632: 	ref_store_init_fn *init;
     633: 	ref_init_db_fn *init_db;
     634: 
     635: 	ref_transaction_prepare_fn *transaction_prepare;
     636: 	ref_transaction_finish_fn *transaction_finish;
     637: 	ref_transaction_abort_fn *transaction_abort;
     638: 	ref_transaction_commit_fn *initial_transaction_commit;
     639: 
     640: 	pack_refs_fn *pack_refs;
     641: 	create_symref_fn *create_symref;
     642: 	delete_refs_fn *delete_refs;
     643: 	rename_ref_fn *rename_ref;
     644: 	copy_ref_fn *copy_ref;
     645: 
     646: 	ref_iterator_begin_fn *iterator_begin;
     647: 	read_raw_ref_fn *read_raw_ref;
     648: 
     649: 	reflog_iterator_begin_fn *reflog_iterator_begin;
     650: 	for_each_reflog_ent_fn *for_each_reflog_ent;
     651: 	for_each_reflog_ent_reverse_fn *for_each_reflog_ent_reverse;
     652: 	reflog_exists_fn *reflog_exists;
     653: 	create_reflog_fn *create_reflog;
     654: 	delete_reflog_fn *delete_reflog;
     655: 	reflog_expire_fn *reflog_expire;
     656: };



# DISCLAIMER

This is not an official Google product
