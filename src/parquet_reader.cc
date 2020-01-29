// Copyright 2017 Luca-SAS, licensed under the Apache License 2.0

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

using parquet::Type;
using parquet::LogicalType;
using parquet::schema::NodePtr;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

Nan::Persistent<Function> ParquetReader::constructor;

ParquetReader::ParquetReader(const Nan::FunctionCallbackInfo<Value>& info) : parquet_file_reader_(), column_readers_({}) {
  if (!info[0]->IsString()) {
    Nan::ThrowTypeError("wrong argument");
    return;
  }
  String::Utf8Value param1(v8::Isolate::GetCurrent(), info[0]->ToString(Nan::GetCurrentContext()).FromMaybe(v8::Local<v8::String>()));
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

  constructor.Reset(Nan::GetFunction(tpl).ToLocalChecked());
  Nan::Set(exports, Nan::New("ParquetReader").ToLocalChecked(), Nan::GetFunction(tpl).ToLocalChecked());
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

// Walk the parquet schema tree to recursively build a javascript object
static void walkSchema(const NodePtr& node, Local<Object> res) {
  Local<Object> obj = Nan::New<Object>();
  Nan::Set(res, Nan::New(node->name().c_str()).ToLocalChecked(), obj);
  if (node->is_optional()) {
    Nan::Set(obj, Nan::New("optional").ToLocalChecked(), Nan::New<Boolean>(node->is_optional()));
  }
  if (node->is_group()) {
    const GroupNode* group = static_cast<const GroupNode*>(node.get());
    for (int i = 0, len = group->field_count(); i < len; i++) {
      walkSchema(group->field(i), obj);
    }
    return;
  }
  const PrimitiveNode* primitive = static_cast<const PrimitiveNode*>(node.get());
  switch (primitive->physical_type()) {
  case Type::BOOLEAN:
    Nan::Set(obj, Nan::New("type").ToLocalChecked(), Nan::New("bool").ToLocalChecked());
    break;
  case Type::INT32:
    Nan::Set(obj, Nan::New("type").ToLocalChecked(), Nan::New("int32").ToLocalChecked());
    break;
  case Type::INT64:
    if (node->logical_type() == LogicalType::TIMESTAMP_MILLIS) {
      Nan::Set(obj, Nan::New("type").ToLocalChecked(), Nan::New("timestamp").ToLocalChecked());
    } else {
      Nan::Set(obj, Nan::New("type").ToLocalChecked(), Nan::New("int64").ToLocalChecked());
    }
    break;
  case Type::INT96:
    Nan::Set(obj, Nan::New("type").ToLocalChecked(), Nan::New("int96").ToLocalChecked());
    break;
  case Type::FLOAT:
    Nan::Set(obj, Nan::New("type").ToLocalChecked(), Nan::New("float").ToLocalChecked());
    break;
  case Type::DOUBLE:
    Nan::Set(obj, Nan::New("type").ToLocalChecked(), Nan::New("double").ToLocalChecked());
    break;
  case Type::BYTE_ARRAY:
    if (node->logical_type() == LogicalType::UTF8) {
      Nan::Set(obj, Nan::New("type").ToLocalChecked(), Nan::New("string").ToLocalChecked());
    } else {
      Nan::Set(obj, Nan::New("type").ToLocalChecked(), Nan::New("byte_array").ToLocalChecked());
    }
    break;
  case Type::FIXED_LEN_BYTE_ARRAY:
    Nan::Set(obj, Nan::New("type").ToLocalChecked(), Nan::New("flba").ToLocalChecked());
    break;
  }
}

void ParquetReader::Info(const Nan::FunctionCallbackInfo<Value>& info) {
  ParquetReader* obj = ObjectWrap::Unwrap<ParquetReader>(info.Holder());
  std::shared_ptr<parquet::FileMetaData> file_metadata = obj->parquet_file_reader_->metadata();
  Local<Object> res = Nan::New<Object>();
  std::string s(file_metadata->created_by());
  const NodePtr root = file_metadata->schema()->schema_root();

  Nan::Set(res, Nan::New("version").ToLocalChecked(), Nan::New<Number>(file_metadata->version()));
  Nan::Set(res, Nan::New("createdBy").ToLocalChecked(), Nan::New(s.c_str()).ToLocalChecked());
  Nan::Set(res, Nan::New("rowGroups").ToLocalChecked(), Nan::New<Number>(file_metadata->num_row_groups()));
  Nan::Set(res, Nan::New("columns").ToLocalChecked(), Nan::New<Number>(file_metadata->num_columns()));
  Nan::Set(res, Nan::New("rows").ToLocalChecked() , Nan::New<Number>(file_metadata->num_rows()));
  walkSchema(root, res);

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
  int16_t definition = maxdef;
  int16_t repetition = maxrep;
  if (!reader->HasNext())
    return;
  reader->ReadBatch(1, &definition, &repetition, &value, &value_read);
  if (maxrep == 0) {
    if (definition == maxdef)
      info.GetReturnValue().Set(Nan::New<V>(value));
    return;
  }
  Local<Array> array = Nan::New<Array>(3);
  Nan::Set(array, Nan::New<Number>(0), Nan::New<Number>(definition));
  Nan::Set(array, Nan::New<Number>(1), Nan::New<Number>(repetition));
  if (definition == maxdef)
    Nan::Set(array, Nan::New<Number>(2), Nan::New<V>(value));
  info.GetReturnValue().Set(array);
}

template <>
void reader<parquet::Int96Reader*, parquet::Int96, Number>(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, const Nan::FunctionCallbackInfo<Value>& info) {
  parquet::Int96Reader* reader = static_cast<parquet::Int96Reader*>(column_reader.get());
  parquet::Int96 value;
  int64_t value_read;
  int16_t definition = maxdef;
  int16_t repetition = maxrep;
  if (!reader->HasNext())
    return;
  reader->ReadBatch(1, &definition, &repetition, &value, &value_read);
  if (maxrep == 0) {
    if (definition == maxdef)
      info.GetReturnValue().Set(Nan::CopyBuffer((char*)value.value, 12).ToLocalChecked());
    return;
  }
  Local<Array> array = Nan::New<Array>(3);
  Nan::Set(array, Nan::New<Number>(0), Nan::New<Number>(definition));
  Nan::Set(array, Nan::New<Number>(1), Nan::New<Number>(repetition));
  if (definition == maxdef)
    Nan::Set(array, Nan::New<Number>(2), Nan::CopyBuffer((char*)value.value, 12).ToLocalChecked());
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
  Nan::Set(array, Nan::New<Number>(0), Nan::New<Number>(definition));
  Nan::Set(array, Nan::New<Number>(1), Nan::New<Number>(repetition));
  if (definition == maxdef)
    Nan::Set(array, Nan::New<Number>(2), Nan::New((char*)value.ptr, value.len).ToLocalChecked());
  info.GetReturnValue().Set(array);
}

template <>
void reader<parquet::FixedLenByteArrayReader*, parquet::FixedLenByteArray, Number>(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, const Nan::FunctionCallbackInfo<Value>& info) {
  parquet::FixedLenByteArrayReader* reader = static_cast<parquet::FixedLenByteArrayReader*>(column_reader.get());
  parquet::FixedLenByteArray value;
  int64_t value_read;
  int16_t definition = maxdef;
  int16_t repetition = maxrep;
  if (!reader->HasNext())
    return;
  reader->ReadBatch(1, &definition, &repetition, &value, &value_read);
  if (maxrep == 0) {
    if (definition == maxdef)
      info.GetReturnValue().Set(Nan::New((char*)value.ptr, 1).ToLocalChecked());
    return;
  }
  Local<Array> array = Nan::New<Array>(3);
  Nan::Set(array, Nan::New<Number>(0), Nan::New<Number>(definition));
  Nan::Set(array, Nan::New<Number>(1), Nan::New<Number>(repetition));
  if (definition == maxdef)
    Nan::Set(array, Nan::New<Number>(2), Nan::New((char*)value.ptr, 1).ToLocalChecked());
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
    int col = info[0]->ToInteger(Nan::GetCurrentContext()).ToLocalChecked()->Value();
    std::shared_ptr<parquet::ColumnReader> column_reader = obj->column_readers_[col];
    const parquet::ColumnDescriptor* descr = column_reader->descr();
    reader_t type_reader = type_readers[column_reader->type()];
    type_reader(column_reader, descr->max_definition_level(), descr->max_repetition_level(), info);
  } catch (const std::exception& e) {
    Nan::ThrowError(Nan::New(e.what()).ToLocalChecked());
    return;
  }
}
