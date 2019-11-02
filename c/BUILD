cc_library(
	name= "reftable",
	srcs = ["record.c",
	"basics.c",
	"slice.c",
	"block.c",
        "reader.c",
	"writer.c",
        "tree.c",
	],
	hdrs = ["api.h",
	"block.h",
	"constants.h",
	"basics.h",
	"record.h",
	"slice.h",
                "tree.h",
	"writer.h",
	],
    copts = [ "-Wall", "-Werror" ],
)

cc_library(
	name= "testlib",
	srcs = ["test_framework.c"],
	hdrs = ["test_framework.h"],
)

cc_test(
	name= "record_test",
	srcs = ["record_test.c"],
	deps = [ ":testlib", ":reftable" ],
)

cc_test(
	name= "tree_test",
	srcs = ["tree_test.c"],
	deps = [ ":testlib", ":reftable" ],
)

cc_test(
	name= "block_test",
	srcs = ["block_test.c"],
	deps = [ ":testlib", ":reftable" ],
)
