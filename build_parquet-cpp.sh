#!/bin/sh -e

mkdir -p build_deps/parquet-cpp
cd build_deps/parquet-cpp
cmake -DPARQUET_BUILD_TESTS=Off \
  -DPARQUET_MINIMAL_DEPENDENCY=ON \
  -DCMAKE_BUILD_TYPE=Release \
  ../../deps/parquet-cpp
make
