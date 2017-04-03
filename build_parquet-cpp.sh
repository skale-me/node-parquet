#!/bin/sh -e

mkdir -p build_deps/parquet-cpp
cd build_deps/parquet-cpp
cmake -DPARQUET_BUILD_TESTS=Off \
  -DPARQUET_BUILD_SHARED=Off -DPARQUET_BUILD_EXECUTABLES=Off \
  -DCMAKE_BUILD_TYPE=Release -DPARQUET_ARROW_LINKAGE=static \
  ../../deps/parquet-cpp
make
