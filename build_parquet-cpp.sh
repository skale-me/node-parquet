#!/bin/sh

SRC_DIR=$PWD/deps/parquet-cpp BUILD_DIR=$PWD/build_deps/parquet-cpp
source deps/parquet-cpp/setup_build_env.sh
cd $BUILD_DIR
cmake -DPARQUET_BUILD_TESTS=Off -DCMAKE_BUILD_TYPE=Release $SRC_DIR 
make
