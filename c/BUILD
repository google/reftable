cc_library(
    name = "reftable",
    srcs = [
        "basics.c",
        "block.c",
        "iter.c",
	"merged.c",
	"pq.c",
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
        "iter.h",
	"pq.h",
        "reader.h",
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
