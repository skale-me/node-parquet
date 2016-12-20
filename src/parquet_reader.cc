#include <iostream>
#include "parquet_reader.h"

v8::Persistent<v8::Function> ParquetReader::constructor;

ParquetReader::ParquetReader(const v8::FunctionCallbackInfo<v8::Value>& args) : pr_(NULL) {
	v8::String::Utf8Value param1(args[0]->ToString());
	std::string from = std::string(*param1);

	pr_ = parquet::ParquetFileReader::OpenFile(from);
	std::cout << "from: " << from << std::endl;
}

ParquetReader::~ParquetReader() {}

void ParquetReader::Init(v8::Isolate* isolate) {
	v8::Local<v8::FunctionTemplate> tpl = v8::FunctionTemplate::New(isolate, New);

	tpl->SetClassName(v8::String::NewFromUtf8(isolate, "ParquetReader"));
	tpl->InstanceTemplate()->SetInternalFieldCount(1);

	NODE_SET_PROTOTYPE_METHOD(tpl, "info", Info);

	constructor.Reset(isolate, tpl->GetFunction());
}

void ParquetReader::New(const v8::FunctionCallbackInfo<v8::Value>& args) {
	ParquetReader* obj = new ParquetReader(args);
	obj->Wrap(args.This());

	args.GetReturnValue().Set(args.This());
}

void ParquetReader::NewInstance(const v8::FunctionCallbackInfo<v8::Value>& args) {
  v8::Isolate* isolate = args.GetIsolate();
  const unsigned argc = 1;
  v8::Local<v8::Value> argv[argc] = { args[0] };
  v8::Local<v8::Function> cons = v8::Local<v8::Function>::New(isolate, constructor);
  v8::Local<v8::Context> context = isolate->GetCurrentContext();
  v8::Local<v8::Object> instance = cons->NewInstance(context, argc, argv).ToLocalChecked();

  args.GetReturnValue().Set(instance);
}

void ParquetReader::Info(const v8::FunctionCallbackInfo<v8::Value>& args) {
  v8::Isolate* isolate = args.GetIsolate();
  v8::Local<v8::String> name;
  std::list<int> columns;
  bool print_values = true;

  ParquetReader* obj = ObjectWrap::Unwrap<ParquetReader>(args.Holder());
  obj->pr_->DebugPrint(std::cout, columns, print_values);

  //args.GetReturnValue().Set(name);
  args.GetReturnValue().Set(v8::String::NewFromUtf8(isolate, "Hello"));
}
