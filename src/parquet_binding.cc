#include <nan.h>

#include "parquet_reader.h"
#include "parquet_writer.h"

NAN_MODULE_INIT(Init) {
  ParquetReader::Init(target);
  ParquetWriter::Init(target);
  Nan::Export(target, "createReader", ParquetReader::NewInstance);
  Nan::Export(target, "createWriter", ParquetWriter::NewInstance);
}

NODE_MODULE(parquet, Init)
