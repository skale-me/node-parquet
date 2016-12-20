#!/bin/sh
mkdir -p build/deps/parquet-cpp
cd build/deps/parquet-cpp
cmake ../../../deps/parquet-cpp
make
