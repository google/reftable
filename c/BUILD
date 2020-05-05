# Mirror core-git COPTS so reftable compiles without warning in CGit.
GIT_COPTS = [
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
    "-Wno-unused-parameter",
]

cc_library(
    name = "reftable",
    srcs = [
        "basics.c",
        "block.c",
        "compat.c",
        "file.c",
        "iter.c",
        "merged.c",
        "pq.c",
        "reader.c",
        "record.c",
        "refname.c",
        "reftable.c",
        "slice.c",
        "stack.c",
        "tree.c",
        "writer.c",
        "zlib-compat.c",
        "basics.h",
        "block.h",
        "compat.h",
        "constants.h",
        "iter.h",
        "merged.h",
        "pq.h",
        "reader.h",
        "refname.h",
        "record.h",
        "slice.h",
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
    srcs = ["test_framework.c"],
    hdrs = ["test_framework.h"],
    deps = [":reftable"],
)

cc_test(
    name = "record_test",
    srcs = ["record_test.c"],
    deps = [
        ":reftable",
        ":testlib",
    ],
)

cc_test(
    name = "reftable_test",
    srcs = ["reftable_test.c"],
    deps = [
        ":reftable",
        ":testlib",
    ],
)

cc_test(
    name = "slice_test",
    srcs = ["slice_test.c"],
    deps = [
        ":reftable",
        ":testlib",
    ],
)

cc_test(
    name = "stack_test",
    srcs = ["stack_test.c"],
    deps = [
        ":reftable",
        ":testlib",
    ],
)

cc_test(
    name = "tree_test",
    srcs = ["tree_test.c"],
    deps = [
        ":reftable",
        ":testlib",
    ],
)

cc_test(
    name = "block_test",
    srcs = ["block_test.c"],
    deps = [
        ":reftable",
        ":testlib",
    ],
)

cc_test(
    name = "refname_test",
    srcs = ["refname_test.c"],
    deps = [
        ":reftable",
        ":testlib",
    ],
)

cc_test(
    name = "merged_test",
    srcs = ["merged_test.c"],
    deps = [
        ":reftable",
        ":testlib",
    ],
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
           "slice_test",
           "stack_test"]]
