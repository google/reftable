# Mirror core-git COPTS so reftable compiles without warning in CGit.
GIT_COPTS = [
    "-DREFTABLE_STANDALONE",
    "-Wall",
    "-Werror",
    "-Wdeclaration-after-statement",
    "-Wstrict-prototypes",
    "-Wformat-security",
    "-Wno-format-zero-length",
    "-Wold-style-definition",
    "-Woverflow",
    "-Wpointer-arith",
    "-Wstrict-prototypes",
    "-Wunused",
    "-Wvla",
    "-Wextra",
    "-Wmissing-prototypes",
    "-Wno-empty-body",
    "-Wno-missing-field-initializers",
    "-Wno-sign-compare",
    "-Werror=strict-aliasing",
    "-Wno-unused-parameter",
]

# Used while in review in git-core upstream, to guarantee buildability of the
# subset offered for review.
cc_library(
    name = "upstream",
    srcs = [
        "basics.c",
        "blocksource.c",
        "compat.c",
        "publicbasics.c",
        "strbuf.c",
        "basics.h",
        "blocksource.h",
        "compat.h",
        "strbuf.h",
        "system.h",
    ],
    hdrs = [
        "include/reftable.h",
    ],
    includes = [
        "include",
    ],
    copts = [
        "-fvisibility=protected",
    ] + GIT_COPTS,
    deps = ["@zlib"],
    visibility = ["//visibility:public"]
)

cc_library(
    name = "reftable",
    srcs = [
        "basics.c",
        "block.c",
        "blocksource.c",
        "compat.c",
        "iter.c",
        "merged.c",
        "pq.c",
        "publicbasics.c",
        "reader.c",
        "record.c",
        "refname.c",
        "reftable.c",
        "strbuf.c",
        "stack.c",
        "tree.c",
        "writer.c",
        "zlib-compat.c",
        "basics.h",
        "block.h",
        "blocksource.h",
        "compat.h",
        "constants.h",
        "iter.h",
        "merged.h",
        "pq.h",
        "reader.h",
        "refname.h",
        "record.h",
        "strbuf.h",
        "stack.h",
        "system.h",
        "tree.h",
        "writer.h",
    ],
    hdrs = [
        "include/reftable.h",
    ],
    includes = [
        "include",
    ],
    copts = [
        "-fvisibility=protected",
    ] + GIT_COPTS,
    deps = ["@zlib"],
    visibility = ["//visibility:public"]
)

cc_library(
    name = "testlib",
    srcs = [
        "test_framework.c",
        "dump.c",
    ],
    hdrs = ["test_framework.h",
            "include/reftable-tests.h",
    ],
    copts = GIT_COPTS,
    deps = [":reftable"],
    visibility = ["//visibility:public"]
)

cc_test(
    name = "record_test",
    srcs = ["record_test.c"],
    deps = [
        ":reftable",
        ":testlib",
    ],
    copts = [
        "-Drecord_test_main=main",
        "-fvisibility=protected",
    ] + GIT_COPTS,
)

cc_test(
    name = "reftable_test",
    srcs = ["reftable_test.c"],
    deps = [
        ":reftable",
        ":testlib",
    ],
    copts = [
        "-Dreftable_test_main=main",
        "-fvisibility=protected",
    ] + GIT_COPTS,
)

cc_test(
    name = "strbuf_test",
    srcs = ["strbuf_test.c"],
    deps = [
        ":reftable",
        ":testlib",
    ] ,
    copts = [
        "-Dstrbuf_test_main=main",
        "-fvisibility=protected",
    ] + GIT_COPTS,
)

cc_test(
    name = "stack_test",
    srcs = ["stack_test.c"],
    deps = [
        ":reftable",
        ":testlib",
    ],
    copts = [
        "-Dstack_test_main=main",
        "-fvisibility=protected",
    ] + GIT_COPTS,
)

cc_test(
    name = "tree_test",
    srcs = ["tree_test.c"],
    deps = [
        ":reftable",
        ":testlib",
    ],
    copts = [
        "-Dtree_test_main=main",
        "-fvisibility=protected",
    ] + GIT_COPTS,
)

cc_test(
    name = "block_test",
    srcs = ["block_test.c"],
    deps = [
        ":reftable",
        ":testlib",
    ],
    copts = [
        "-Dblock_test_main=main",
        "-fvisibility=protected",
    ] + GIT_COPTS,
)

cc_test(
    name = "refname_test",
    srcs = ["refname_test.c"],
    deps = [
        ":reftable",
        ":testlib",
    ],
    copts = [
        "-Drefname_test_main=main",
        "-fvisibility=protected",
    ] + GIT_COPTS,
)

cc_test(
    name = "merged_test",
    srcs = ["merged_test.c"],
    deps = [
        ":reftable",
        ":testlib",
    ],
    copts = [
        "-Dmerged_test_main=main",
        "-fvisibility=protected",
    ] + GIT_COPTS,
)

[sh_test(
    name = "%s_valgrind_test" % t,
    srcs = [ "valgrind_test.sh" ],
    args = [ t ],
    data = [ t ])
 for t in ["record_test",
           "merged_test",
           "refname_test",
           "tree_test",
           "block_test",
           "strbuf_test",
           "stack_test"]]
