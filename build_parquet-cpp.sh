#!/bin/sh -e

BUILD_DIR=$PWD/build_deps/parquet-cpp
ARROW_EP=$BUILD_DIR/arrow_ep-prefix/src/arrow_ep-build
BROTLI_EP=$ARROW_EP/brotli_ep-prefix/src/brotli_ep-build

export SNAPPY_STATIC_LIB=$ARROW_EP/snappy_ep/src/snappy_ep-install/lib/libsnappy.a
export BROTLI_STATIC_LIB_ENC=$BROTLI_EP/libbrotlienc.a
export BROTLI_STATIC_LIB_DEC=$BROTLI_EP/libbrotlidec.a
export BROTLI_STATIC_LIB_COMMON=$BROTLI_EP/libbrotlicommon.a
export ZLIB_STATIC_LIB=$ARROW_EP/zlib_ep/src/zlib_ep-install/lib/libz.a

mkdir -p $BUILD_DIR
cd $BUILD_DIR
cmake -DPARQUET_BUILD_TESTS=OFF \
  -DPARQUET_MINIMAL_DEPENDENCY=ON \
  -DPARQUET_ARROW_LINKAGE="static" \
  -DCMAKE_BUILD_TYPE=Release \
  ../../deps/parquet-cpp
make
