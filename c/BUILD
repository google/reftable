
cc_library(
	name= "reftable",
	srcs = ["record.c"],
	hdrs = ["api.h",
	"record.h",

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
