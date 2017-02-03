#!/bin/sh -e

mkdir -p build_deps/parquet-cpp
cd build_deps/parquet-cpp
cmake --debug-output -DPARQUET_BUILD_TESTS=Off -DCMAKE_BUILD_TYPE=Release ../../deps/parquet-cpp
make
