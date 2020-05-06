#!/bin/bash

BAZELRC_FILE="$(bazel info workspace)/clang.bazelrc"

LLVM_PREFIX=$1

if [[ ! -e "${LLVM_PREFIX}/bin/llvm-config" ]]; then
  echo "Error: cannot find llvm-config in ${LLVM_PREFIX}."
  exit 1
fi

export PATH="$(${LLVM_PREFIX}/bin/llvm-config --bindir):${PATH}"

echo "# Generated file, do not edit. If you want to disable clang, just delete this file.
build:clang --action_env=PATH=${PATH}
build:clang --action_env=CC=clang
build:clang --action_env=CXX=clang++
build:clang --action_env=LLVM_CONFIG=${LLVM_PREFIX}/bin/llvm-config
build:clang --repo_env=LLVM_CONFIG=${LLVM_PREFIX}/bin/llvm-config
build:clang --linkopt=-L$(llvm-config --libdir)
build:clang --linkopt=-Wl,-rpath,$(llvm-config --libdir)

" > ${BAZELRC_FILE}
