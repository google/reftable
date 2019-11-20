This is a from scratch implementation of the reftable format for
storing Git ref and reflog data. 

The spec is at 
https://eclipse.googlesource.com/jgit/jgit/+/refs/heads/master/Documentation/technical/reftable.md

Based on the implementation, there is also [a proposal for a v2 of the
format](reftable-v2-proposal.md).


GO
==

The Go implementation implements the spec completely

C
=

An experimental implementation in C is under the directory c/ . It
implements ref records, the obj index, and merged reftables. It lacks
support for reflog blocks.

DISCLAIMER
==========

This is not an official Google product
