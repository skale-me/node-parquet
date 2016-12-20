#include <iostream>
#include "parquet_reader.h"

using v8::Context;
using v8::Function;
using v8::FunctionTemplate;
using v8::FunctionCallbackInfo;
using v8::Isolate;
using v8::Local;
using v8::Number;
using v8::Object;
using v8::Persistent;
using v8::String;
using v8::Value;

Persistent<Function> ParquetReader::constructor;

ParquetReader::ParquetReader(const FunctionCallbackInfo<Value>& args) : pr_(nullptr) {
	String::Utf8Value param1(args[0]->ToString());
	std::string from = std::string(*param1);

	pr_ = parquet::ParquetFileReader::OpenFile(from);
	std::cout << "from: " << from << std::endl;
}

ParquetReader::~ParquetReader() {}

void ParquetReader::Init(Isolate* isolate) {
	Local<FunctionTemplate> tpl = FunctionTemplate::New(isolate, New);

	tpl->SetClassName(String::NewFromUtf8(isolate, "ParquetReader"));
	tpl->InstanceTemplate()->SetInternalFieldCount(1);

	NODE_SET_PROTOTYPE_METHOD(tpl, "info", Info);
	NODE_SET_PROTOTYPE_METHOD(tpl, "debugPrint", DebugPrint);

	constructor.Reset(isolate, tpl->GetFunction());
}

void ParquetReader::New(const FunctionCallbackInfo<Value>& args) {
	ParquetReader* obj = new ParquetReader(args);
	obj->Wrap(args.This());

	args.GetReturnValue().Set(args.This());
}

void ParquetReader::NewInstance(const FunctionCallbackInfo<Value>& args) {
  Isolate* isolate = args.GetIsolate();
  const unsigned argc = 1;
  Local<Value> argv[argc] = { args[0] };
  Local<Function> cons = Local<Function>::New(isolate, constructor);
  Local<Context> context = isolate->GetCurrentContext();
  Local<Object> instance = cons->NewInstance(context, argc, argv).ToLocalChecked();

  args.GetReturnValue().Set(instance);
}

void ParquetReader::DebugPrint(const FunctionCallbackInfo<Value>& args) {
  Isolate* isolate = args.GetIsolate();
  ParquetReader* obj = ObjectWrap::Unwrap<ParquetReader>(args.Holder());
  std::list<int> columns;
  bool print_values = true;

  obj->pr_->DebugPrint(std::cout, columns, print_values);
  args.GetReturnValue().Set(String::NewFromUtf8(isolate, "Hello"));
}

void ParquetReader::Info(const FunctionCallbackInfo<Value>& args) {
  Isolate* isolate = args.GetIsolate();
  ParquetReader* obj = ObjectWrap::Unwrap<ParquetReader>(args.Holder());
  const parquet::FileMetaData* file_metadata = obj->pr_->metadata();
  Local<Object> res = Object::New(isolate);
  std::string s(file_metadata->created_by());

  res->Set(String::NewFromUtf8(isolate, "version"), Number::New(isolate, file_metadata->version()));
  res->Set(String::NewFromUtf8(isolate, "createdBy"), String::NewFromUtf8(isolate, s.c_str()));
  res->Set(String::NewFromUtf8(isolate, "rowGroups"), Number::New(isolate, file_metadata->num_row_groups()));
  res->Set(String::NewFromUtf8(isolate, "columns"), Number::New(isolate, file_metadata->num_columns()));
  res->Set(String::NewFromUtf8(isolate, "rows"), Number::New(isolate, file_metadata->num_rows()));

  args.GetReturnValue().Set(res);
}
