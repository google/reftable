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
        "file.c",
        "iter.c",
        "merged.c",
        "pq.c",
        "reader.c",
        "record.c",
        "slice.c",
        "stack.c",
        "tree.c",
        "writer.c",
        "zlib-compat.c",
    ],
    hdrs = [
        "basics.h",
        "block.h",
        "blocksource.h",
        "config.h",
        "constants.h",
        "iter.h",
        "merged.h",
        "pq.h",
        "reader.h",
        "record.h",
        "reftable.h",
        "slice.h",
        "stack.h",
        "system.h",
        "tree.h",
        "writer.h",
    ],
    copts = [
        "-fvisibility=protected",
    ] + GIT_COPTS,
    deps = ["@zlib"],
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
    name = "merged_test",
    srcs = ["merged_test.c"],
    deps = [
        ":reftable",
        ":testlib",
    ],
)

cc_binary(
    name = "dump",
    srcs = ["dump.c"],
    deps = [":reftable"],
)
