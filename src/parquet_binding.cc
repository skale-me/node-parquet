// Copyright 2017 Luca-SAS, licensed under the Apache License 2.0

#include <nan.h>

#include "parquet_reader.h"
#include "parquet_writer.h"

NAN_MODULE_INIT(Init) {
  ParquetReader::Init(target);
  ParquetWriter::Init(target);
}

NODE_MODULE(parquet, Init)
