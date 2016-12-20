#include <node.h>

#include "parquet_reader.h"

void CreateReader(const v8::FunctionCallbackInfo<v8::Value>& args) {
	ParquetReader::NewInstance(args);
}

void Init(v8::Local<v8::Object> exports) {
	ParquetReader::Init(exports->GetIsolate());

	NODE_SET_METHOD(exports, "createReader", CreateReader);
}

NODE_MODULE(parquet, Init)
