#!/bin/sh
set -eu
tests=$(ls -1 c/*_test.c | sed 's|\.c$||' )
nontest=$(ls -1 c/*.c | grep -v test)

export BAZEL_USE_LLVM_NATIVE_COVERAGE=1
export GCOV=llvm-profdata

bazel --bazelrc clang.bazelrc coverage --config=clang \
  ${tests}

OBJECTS=""
for t in ${tests}; do
    OBJECTS="${OBJECTS} bazel-bin/$t"
done

mkdir -p coverage
llvm-profdata merge -sparse \
		  -o coverage/coverage.dat \
		  $(find -L $(bazel info bazel-testlogs) -name coverage.dat )

llvm-cov show -instr-profile=coverage/coverage.dat \
	     bazel-bin/c/libreftable.so  -Xdemangler=c++filt \
	     -output-dir=coverage/coverage.dat -format=html

sed -i -e 's|>proc/self/cwd/|>|g' "coverage/index.html"
