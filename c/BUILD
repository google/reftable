
cc_library(
	name= "reftable",
	srcs = ["record.c",
	"basics.c",
	"slice.c",
	"block.c",
	"writer.c",
	],
	hdrs = ["api.h",
	"block.h",
	"constants.h",
	"basics.h",
	"record.h",
	"slice.h",
	"writer.h",
	],
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
	name= "block_test",
	srcs = ["block_test.c"],
	deps = [ ":testlib", ":reftable" ],
)
