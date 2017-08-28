#include <iostream>
#include <vector>

#include "parquet_reader.h"

using v8::Array;
using v8::Boolean;
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

ParquetReader::ParquetReader(const Nan::FunctionCallbackInfo<Value>& info) : parquet_file_reader_(), column_readers_({}) {
  if (!info[0]->IsString()) {
    Nan::ThrowTypeError("wrong argument");
    return;
  }
  String::Utf8Value param1(info[0]->ToString());
  std::string from = std::string(*param1);

  try {
    parquet_file_reader_ = parquet::ParquetFileReader::OpenFile(from);
    std::shared_ptr<parquet::RowGroupReader> row_group_reader= parquet_file_reader_->RowGroup(0);
    int num_columns = parquet_file_reader_->metadata()->num_columns();
    for (int i = 0; i < num_columns; i++) {
      column_readers_.push_back(row_group_reader->Column(i));
    }
  } catch (const std::exception& e) {
    Nan::ThrowError(Nan::New(e.what()).ToLocalChecked());
  }
}

ParquetReader::~ParquetReader() {}

void ParquetReader::Init(Local<Object> exports) {
  Nan::HandleScope scope;

  Local<FunctionTemplate> tpl = Nan::New<FunctionTemplate>(New);

  tpl->SetClassName(Nan::New("ParquetReader").ToLocalChecked());
  tpl->InstanceTemplate()->SetInternalFieldCount(1);

  Nan::SetPrototypeMethod(tpl, "info", Info);
  Nan::SetPrototypeMethod(tpl, "read", Read);
  Nan::SetPrototypeMethod(tpl, "close", Close);

  constructor.Reset(tpl->GetFunction());
  exports->Set(Nan::New("ParquetReader").ToLocalChecked(), tpl->GetFunction());
}

void ParquetReader::New(const Nan::FunctionCallbackInfo<Value>& info) {
  ParquetReader* obj = new ParquetReader(info);
  obj->Wrap(info.This());
  info.GetReturnValue().Set(info.This());
}

void ParquetReader::NewInstance(const Nan::FunctionCallbackInfo<Value>& info) {
  const int argc = 1;
  Local<Value> argv[argc] = { info[0] };
  Local<Function> cons = Nan::New<v8::Function>(constructor);
  info.GetReturnValue().Set(Nan::NewInstance(cons, argc, argv).ToLocalChecked());
}

void ParquetReader::Info(const Nan::FunctionCallbackInfo<Value>& info) {
  ParquetReader* obj = ObjectWrap::Unwrap<ParquetReader>(info.Holder());
  std::shared_ptr<parquet::FileMetaData> file_metadata = obj->parquet_file_reader_->metadata();
  Local<Object> res = Nan::New<Object>();
  std::string s(file_metadata->created_by());

  res->Set(Nan::New("version").ToLocalChecked(), Nan::New<Number>(file_metadata->version()));
  res->Set(Nan::New("createdBy").ToLocalChecked(), Nan::New(s.c_str()).ToLocalChecked());
  res->Set(Nan::New("rowGroups").ToLocalChecked(), Nan::New<Number>(file_metadata->num_row_groups()));
  res->Set(Nan::New("columns").ToLocalChecked(), Nan::New<Number>(file_metadata->num_columns()));
  res->Set(Nan::New("rows").ToLocalChecked() , Nan::New<Number>(file_metadata->num_rows()));

  info.GetReturnValue().Set(res);
}

void ParquetReader::Close(const Nan::FunctionCallbackInfo<Value>& info) {
  ParquetReader* obj = ObjectWrap::Unwrap<ParquetReader>(info.Holder());
  obj->parquet_file_reader_->Close();
}

template <typename T, typename U, typename V>
void reader(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, const Nan::FunctionCallbackInfo<Value>& info) {
  T reader = static_cast<T>(column_reader.get());
  U value;
  int64_t value_read;
  int16_t definition;
  int16_t repetition;
  if (!reader->HasNext())
    return;
  reader->ReadBatch(1, &definition, &repetition, &value, &value_read);
  if (maxrep == 0) {
    if (definition == maxdef)
      info.GetReturnValue().Set(Nan::New<V>(value));
    return;
  }
  Local<Array> array = Nan::New<Array>(3);
  array->Set(Nan::New<Number>(0), Nan::New<Number>(definition));
  array->Set(Nan::New<Number>(1), Nan::New<Number>(repetition));
  if (definition == maxdef)
    array->Set(Nan::New<Number>(2), Nan::New<V>(value));
  info.GetReturnValue().Set(array);
}

template <>
void reader<parquet::Int96Reader*, parquet::Int96, Number>(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, const Nan::FunctionCallbackInfo<Value>& info) {
  parquet::Int96Reader* reader = static_cast<parquet::Int96Reader*>(column_reader.get());
  parquet::Int96 value;
  int64_t value_read;
  int16_t definition;
  int16_t repetition;
  if (!reader->HasNext())
    return;
  reader->ReadBatch(1, &definition, &repetition, &value, &value_read);
  if (maxrep == 0) {
    if (definition == maxdef)
      info.GetReturnValue().Set(Nan::CopyBuffer((char*)value.value, 12).ToLocalChecked());
    return;
  }
  Local<Array> array = Nan::New<Array>(3);
  array->Set(Nan::New<Number>(0), Nan::New<Number>(definition));
  array->Set(Nan::New<Number>(1), Nan::New<Number>(repetition));
  if (definition == maxdef)
    array->Set(Nan::New<Number>(2), Nan::CopyBuffer((char*)value.value, 12).ToLocalChecked());
  info.GetReturnValue().Set(array);
}

template <>
void reader<parquet::ByteArrayReader*, parquet::ByteArray, Number>(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, const Nan::FunctionCallbackInfo<Value>& info) {
  parquet::ByteArrayReader* reader = static_cast<parquet::ByteArrayReader*>(column_reader.get());
  parquet::ByteArray value;
  int64_t value_read;
  int16_t definition = maxdef;
  int16_t repetition = maxrep;
  if (!reader->HasNext())
    return;
  reader->ReadBatch(1, &definition, &repetition, &value, &value_read);
  if (maxrep == 0) {
    if (definition == maxdef)
      info.GetReturnValue().Set(Nan::New((char*)value.ptr, value.len).ToLocalChecked());
    return;
  }
  Local<Array> array = Nan::New<Array>(3);
  array->Set(Nan::New<Number>(0), Nan::New<Number>(definition));
  array->Set(Nan::New<Number>(1), Nan::New<Number>(repetition));
  if (definition == maxdef)
    array->Set(Nan::New<Number>(2), Nan::New((char*)value.ptr, value.len).ToLocalChecked());
  info.GetReturnValue().Set(array);
}

template <>
void reader<parquet::FixedLenByteArrayReader*, parquet::FixedLenByteArray, Number>(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, const Nan::FunctionCallbackInfo<Value>& info) {
  parquet::FixedLenByteArrayReader* reader = static_cast<parquet::FixedLenByteArrayReader*>(column_reader.get());
  parquet::FixedLenByteArray value;
  int64_t value_read;
  int16_t definition;
  int16_t repetition;
  if (!reader->HasNext())
    return;
  reader->ReadBatch(1, &definition, &repetition, &value, &value_read);
  if (maxrep == 0) {
    if (definition == maxdef)
      info.GetReturnValue().Set(Nan::New((char*)value.ptr, 1).ToLocalChecked());
    return;
  }
  Local<Array> array = Nan::New<Array>(3);
  array->Set(Nan::New<Number>(0), Nan::New<Number>(definition));
  array->Set(Nan::New<Number>(1), Nan::New<Number>(repetition));
  if (definition == maxdef)
    array->Set(Nan::New<Number>(2), Nan::New((char*)value.ptr, 1).ToLocalChecked());
  info.GetReturnValue().Set(array);
}

typedef void (*reader_t)(std::shared_ptr<parquet::ColumnReader>, int16_t, int16_t, const Nan::FunctionCallbackInfo<Value>& info);

// Table of parquet readers. Keep same order as in parquet::Type
static reader_t type_readers[] = {
  reader<parquet::BoolReader*, bool, Boolean>,
  reader<parquet::Int32Reader*, int32_t, Number>,
  reader<parquet::Int64Reader*, int64_t, Number>,
  reader<parquet::Int96Reader*, parquet::Int96, Number>,
  reader<parquet::FloatReader*, float, Number>,
  reader<parquet::DoubleReader*, double, Number>,
  reader<parquet::ByteArrayReader*, parquet::ByteArray, Number>,
  reader<parquet::FixedLenByteArrayReader*, parquet::FixedLenByteArray, Number>,
};

// Read one column element.
void ParquetReader::Read(const Nan::FunctionCallbackInfo<Value>& info) {
  ParquetReader* obj = ObjectWrap::Unwrap<ParquetReader>(info.Holder());

  try {
    int col = info[0]->IntegerValue();
    std::shared_ptr<parquet::ColumnReader> column_reader = obj->column_readers_[col];
    const parquet::ColumnDescriptor* descr = column_reader->descr();
    reader_t type_reader = type_readers[column_reader->type()];
    type_reader(column_reader, descr->max_definition_level(), descr->max_repetition_level(), info);
  } catch (const std::exception& e) {
    Nan::ThrowError(Nan::New(e.what()).ToLocalChecked());
    return;
  }
}
