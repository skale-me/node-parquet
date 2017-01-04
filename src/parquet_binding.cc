#include <nan.h>

#include "parquet_reader.h"

void CreateReader(const Nan::FunctionCallbackInfo<v8::Value>& args) {
  ParquetReader::NewInstance(args);
}

void Init(v8::Local<v8::Object> exports, v8::Local<v8::Object> module) {
  ParquetReader::Init(exports);

  Nan::SetMethod(module, "exports", CreateReader);
}

NODE_MODULE(parquet, Init)
