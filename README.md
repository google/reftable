
This is a from scratch implementation of the reftable format for
storing Git ref and reflog data.

The spec is at
https://eclipse.googlesource.com/jgit/jgit/+/refs/heads/master/Documentation/technical/reftable.md

Based on the implementation, there is also [a proposal for a v2 of the
format](reftable-v2-proposal.md).


# GO

The Go implementation implements the spec completely.
[API doc](https://godoc.org/github.com/google/reftable).

# C

An experimental implementation in C is under the directory c/ . It is
functionally equivalent to the Go version. Build and test it using

```
bazel test c/...
```

API is documented in side the [reftable.h
header](https://github.com/google/reftable/blob/master/c/reftable.h).

# Open questions

*   How to deal with HASH_SIZE != 20 ? Will repos exist in dual config, if so
    what is the plan for ref transactions?

*   What is a good strategy for pruning reflogs?

# git-core integration notes

See https://github.com/hanwen/git/tree/reftable.


# Background reading

* Spec: https://github.com/eclipse/jgit/blob/master/Documentation/technical/reftable.md

* Original discussion on JGit-dev:  https://www.eclipse.org/lists/jgit-dev/msg03389.html

* First design discussion on git@vger: https://public-inbox.org/git/CAJo=hJtyof=HRy=2sLP0ng0uZ4=S-DpZ5dR1aF+VHVETKG20OQ@mail.gmail.com/

* Last design discussion on git@vger: https://public-inbox.org/git/CAJo=hJsZcAM9sipdVr7TMD-FD2V2W6_pvMQ791EGCDsDkQ033w@mail.gmail.com/

* First attempt at implementation: https://public-inbox.org/git/CAP8UFD0PPZSjBnxCA7ez91vBuatcHKQ+JUWvTD1iHcXzPBjPBg@mail.gmail.com/

* libgit2 support issue: https://github.com/libgit2/libgit2/issues

* GitLab support issue: https://gitlab.com/gitlab-org/git/issues/6

* go-git support issue: https://github.com/src-d/go-git/issues/1059


# DISCLAIMER

This is not an official Google product
