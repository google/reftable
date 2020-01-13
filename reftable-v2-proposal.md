
Gaps in the spec

* multiple values can be stored for one key; the spec unclear if this
  is legal.  Suggestion: disallow and clarify the spec.

* The block _before_ an unaligned section (either log block or final
  footer) can be unpadded.

Suggestions for improvement of the file format:

* Reftable v1 starts with a 24-byte file header, which requires special
  casing. Headers allow streaming reads of the file, but this is not
  useful for reftable.

* The Footer misses some offsets (start of Ref section)

* MaxUpdateIndex must be specified upfront, but it can be
  automatically computed. MaxUpdateIndex has no use in streaming reads
  either.

* Currently log blocks are encoded as 'g' u24(uncompressed). This
  means navigating among log blocks means having to uncompress them,
  and needs more special handling.

  For log blocks, encode the header as {'g' u24(COMPRESSED) }
  u24(UNCOMPRESSED) }. This makes block handling more uniform across
  block types: you can navigate across log blocks without having to
  uncompress them.

  AFAIK, log blocks aren't in production anywhere, so arguably, we can
  change this without requiring to publish a new standard?

* spec is unclear: if a file has just log blocks, do the log restart
  offsets include the 24 byte file header? They do, but it's not
  completely obvious.

* For reading the next 'r' block, you have to peek in two places, either at the
  start of the padding (next block is a 'g'), or at the next block size.


Problems with Reftable v1
=========================

The table format is extremely optimized, at the price of complexity.
The basic table reader/writer weighs in at 2.5kLOC.  This effort has
to be duplicated across implementations (libgit2, jgit, cgit, go-git,
dulwich etc.)

* MaxUpdateIndex must be specified upfront, but it can be
  autocomputed.

* The log block only specifies the uncompressed block size. It is
  very likely that this is more than the compressed block size, but is
  not guaranteed; the only guaranteed way is to read the block until
  end of file.

* Different sections/keytypes need different handling (flate
  compression for logs, you need to deserialize value to skip among
  keys, aligned vs unaligned blocks)

* Padding between blocks is poorly defined.

* Record types are hardcoded, making backward compatibility more
  difficult, should any extra data types ever need to be stored in
  reftable

* Overall, the format is extremely optimized, but does it by fusing
  the container format (sorted key/value store with prefix compression
  and indices) with business logic (ie. Git specific concepts such as
  log records and ref records). The basic table reader/writer weighs
  in at 2.5kLOC.  This effort has to be duplicated across
  implementations (libgit2, jgit, cgit, go-git, dulwich etc.)

  The overall format would be easier to understand and implement if
  the container format and the business logic.

* padding between blocks poorly defined.

* the footer has no statistics, which is useful for making decisions
  about compacting or not. File size only works if tables are large or
  if tables are unaligned.

If there were ever to be a V2 format, here is a my suggestion for a
format that gets most of the benefits with less complications:


Proposal: Reftable v2 - container format
========================================

There is a key-value storage primitive that is structured as a single
table:

There is no file header.

Blocks have the following format. They are optionally padded to the
table block size:

   uint8 (compression-algorithm)
   uint24(unpadded-block-size)

   Block body

For a uncompressed body, the leading byte is 0.

For a compressed body, the leading byte indicates algorithm (1 = zlib
flate).  The compressed body is laid out as

   uint32(uncompressed-body-size)
   compressed-body

The body is structured as follows:

   key/val data
   uint24(key-restart-data)
   uint16(restarts)
   padding

Keys are prefix-compressed:

  varint(prefix-size) varint(suffix-size << 3 | extra-3 ) suffix

The extra-3 bits are used just as in reftable v1.

Values are:

  varint(payload-size) payload

There is a (multi-)level index, similar to reftable v1. Each table
always contains an index.

The footer is as follows:

  "SSTB"
  uint8 (container-version)
  uint24 (block-size)
  uint64 (first index block offset, or 0 if missing)

  METADATA

  uint16 (footer-size)
  "REFT"
  uint32 (CRC-32 of footer)

"SSTB" is for sorted string table, version 1.

Proposal: Reftable v2 - git storage conventions.
===============================================

Different values types are distinguished by their keys:

Ref keys are

  'a' REF-NAME

Obj keys are

  'b' OBJ-ID

Log keys are

  'c' REF-NAME revInt64(ts)

It is recommended to use compression on blocks that consist of just
reflog data.

METADATA includes the following data:

  GitStorage-version uint8
  MinUpdateIndex uint64
  MaxUpdateIndex uint64

the current GitStorage-version is 2. git-level format changes (eg. new
hash sizes) can be achieved by leaving the container version at 2, but
increasing GitStorage-version and storing more metadata
