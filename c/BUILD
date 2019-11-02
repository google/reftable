cc_library(
    name = "reftable",
    srcs = [
        "basics.c",
        "block.c",
        "iter.c",
        "reader.c",
        "record.c",
        "slice.c",
        "tree.c",
        "writer.c",
    ],
    hdrs = [
        "api.h",
        "basics.h",
        "block.h",
        "constants.h",
        "record.h",
        "slice.h",
        "tree.h",
        "writer.h",
    ],
    copts = [
        "-Wall",
        "-Werror",
    ],
)

cc_library(
    name = "testlib",
    srcs = ["test_framework.c"],
    hdrs = ["test_framework.h"],
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
