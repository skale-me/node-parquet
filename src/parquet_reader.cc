#include <iostream>

#include "parquet_reader.h"

using v8::Context;
using v8::Exception;
using v8::Function;
using v8::FunctionTemplate;
using v8::Isolate;
using v8::Local;
using v8::Number;
using v8::Object;
using v8::String;
using v8::Value;

Nan::Persistent<Function> ParquetReader::constructor;

ParquetReader::ParquetReader(const Nan::FunctionCallbackInfo<Value>& args) : pr_(nullptr) {
  String::Utf8Value param1(args[0]->ToString());
  std::string from = std::string(*param1);

  pr_ = parquet::ParquetFileReader::OpenFile(from);
  std::cout << "from: " << from << std::endl;
}

ParquetReader::~ParquetReader() {}

void ParquetReader::Init(Local<Object> exports) {
  Nan::HandleScope scope;

  Local<FunctionTemplate> tpl = Nan::New<FunctionTemplate>(New);

  tpl->SetClassName(Nan::New("ParquetReader").ToLocalChecked());
  tpl->InstanceTemplate()->SetInternalFieldCount(1);

  Nan::SetPrototypeMethod(tpl, "info", Info);
  Nan::SetPrototypeMethod(tpl, "debugPrint", DebugPrint);
  Nan::SetPrototypeMethod(tpl, "readSync", ReadSync);

  constructor.Reset(tpl->GetFunction());
  constructor.Reset(tpl->GetFunction());
  exports->Set(Nan::New("ParquetReader").ToLocalChecked(), tpl->GetFunction());
}

void ParquetReader::New(const Nan::FunctionCallbackInfo<Value>& args) {
  ParquetReader* obj = new ParquetReader(args);
  obj->Wrap(args.This());

  args.GetReturnValue().Set(args.This());
}

void ParquetReader::NewInstance(const Nan::FunctionCallbackInfo<Value>& args) {
  Isolate* isolate = args.GetIsolate();
  const unsigned argc = 1;
  Local<Value> argv[argc] = { args[0] };
  Local<Function> cons = Local<Function>::New(isolate, constructor);
  Local<Context> context = isolate->GetCurrentContext();
  Local<Object> instance = cons->NewInstance(context, argc, argv).ToLocalChecked();

  args.GetReturnValue().Set(instance);
}

void ParquetReader::DebugPrint(const Nan::FunctionCallbackInfo<Value>& args) {
  Isolate* isolate = args.GetIsolate();
  ParquetReader* obj = ObjectWrap::Unwrap<ParquetReader>(args.Holder());
  std::list<int> columns;
  bool print_values = true;

  obj->pr_->DebugPrint(std::cout, columns, print_values);
  args.GetReturnValue().Set(String::NewFromUtf8(isolate, "Hello"));
}

void ParquetReader::Info(const Nan::FunctionCallbackInfo<Value>& args) {
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

void ParquetReader::ReadSync(const Nan::FunctionCallbackInfo<Value>& args) {
  Isolate* isolate = args.GetIsolate();
  int colnum;
  int start;
  int n;
  ParquetReader* obj = ObjectWrap::Unwrap<ParquetReader>(args.Holder());
  const parquet::FileMetaData* file_metadata = obj->pr_->metadata();
  Local<Object> res = Object::New(isolate);
  Nan::MaybeLocal<v8::Object> buffer = Nan::NewBuffer(10);

  if (args.Length() < 3) {
    isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "wrong number of arguments")));
    return;
  }
  if (!args[0]->IsNumber() || !args[1]->IsNumber() || !args[2]->IsNumber()) {
    isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "wrong argument")));
    return;
  }
  colnum = args[0]->IntegerValue();
  start = args[1]->IntegerValue();
  n = args[2]->IntegerValue();
  std::cout << "colnum: " << colnum << std::endl;
  std::cout << "start: " << start << std::endl;
  std::cout << "n: " << n << std::endl;
  std::cout << "sizeof(bool): " << sizeof(bool) << std::endl;

  args.GetReturnValue().Set(res);
}
