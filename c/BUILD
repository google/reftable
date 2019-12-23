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
    ],
    hdrs = [
        "basics.h",
        "block.h",
        "blocksource.h",
        "constants.h",
        "iter.h",
        "merged.h",
	"pq.h",
        "reader.h",
        "record.h",
        "reftable.h",
        "slice.h",
	"stack.h",
        "tree.h",
        "writer.h",
    ],
    copts = [
        "-Wall",
        "-Werror",
    ],
    deps = [ "@zlib//:zlib" ],
)

cc_library(
    name = "testlib",
    deps = [ ":reftable", ],
    srcs = ["test_framework.c", ],
    hdrs = ["test_framework.h", ],
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



