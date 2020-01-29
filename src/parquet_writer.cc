// Copyright 2017 Luca-SAS, licensed under the Apache License 2.0

#include <iostream>
#include <string>

#include <arrow/io/file.h>

#include "parquet_writer.h"

using parquet::Compression;
using parquet::LogicalType;
using parquet::Repetition;
using parquet::Type;
using parquet::schema::PrimitiveNode;
using parquet::schema::GroupNode;
using parquet::schema::NodePtr;
using parquet::schema::Node;
using parquet::WriterProperties;

using v8::Array;
using v8::Function;
using v8::FunctionTemplate;
using v8::Local;
using v8::Int32;
using v8::Number;
using v8::Object;
using v8::String;
using v8::Value;

static NodePtr SetupSchema(std::string root_name, Repetition::type root_repetition, Local<Object> obj) {
  parquet::schema::NodeVector fields;
  Local<Array> properties = obj->GetOwnPropertyNames(Nan::GetCurrentContext()).ToLocalChecked();
  int len = properties->Length();

  for (int i = 0; i < len; i++) {
    Local<Value> key = Nan::Get(properties, i).ToLocalChecked();
    Local<Object> value = Local<Object>::Cast(Nan::Get(obj, key).ToLocalChecked());
    String::Utf8Value key_utf8(v8::Isolate::GetCurrent(), key->ToString(Nan::GetCurrentContext()).FromMaybe(v8::Local<v8::String>()));
    Local<Value> type = Nan::Get(value, Nan::New("type").ToLocalChecked()).ToLocalChecked();
    Local<Value> optional = Nan::Get(value, Nan::New("optional").ToLocalChecked()).ToLocalChecked();
    Local<Value> repeat = Nan::Get(value, Nan::New("repeat").ToLocalChecked()).ToLocalChecked();
    Local<Object> schema;
    String::Utf8Value type_utf8(v8::Isolate::GetCurrent(), type->ToString(Nan::GetCurrentContext()).FromMaybe(v8::Local<v8::String>()));
    std::string type_str = std::string(*type_utf8);
    std::string key_str = std::string(*key_utf8);
    Type::type parquet_type = Type::BOOLEAN;
    LogicalType::type logical_type = LogicalType::NONE;
    Repetition::type repetition;
    Node::type node_type = Node::PRIMITIVE;

    if (optional->ToBoolean(Nan::GetCurrentContext()).ToLocalChecked()->Value()) {
      repetition = Repetition::OPTIONAL;
    } else {
      if (repeat->ToBoolean(Nan::GetCurrentContext()).ToLocalChecked()->Value()) {
        repetition = Repetition::REPEATED;
      } else {
        repetition = Repetition::REQUIRED;
      }
    }

    if (type_str.compare("bool") == 0) {
      parquet_type = Type::BOOLEAN;
    } else if (type_str.compare("int32") == 0) {
      parquet_type = Type::INT32;
    } else if (type_str.compare("int64") == 0) {
      parquet_type = Type::INT64;
    } else if (type_str.compare("timestamp") == 0) {
      parquet_type = Type::INT64;
      logical_type = LogicalType::TIMESTAMP_MILLIS;
    } else if (type_str.compare("int96") == 0) {
      parquet_type = Type::INT96;
    } else if (type_str.compare("float") == 0) {
      parquet_type = Type::FLOAT;
    } else if (type_str.compare("double") == 0) {
      parquet_type = Type::DOUBLE;
    } else if (type_str.compare("string") == 0) {
      parquet_type = Type::BYTE_ARRAY;
      logical_type = LogicalType::UTF8;
    } else if (type_str.compare("byte_array") == 0) {
      parquet_type = Type::BYTE_ARRAY;
    } else if (type_str.compare("fixed_len_byte_array") == 0) {
      parquet_type = Type::FIXED_LEN_BYTE_ARRAY;
    } else if (type_str.compare("group") == 0) {
      node_type = Node::GROUP;
    } else if (type_str.compare("list") == 0) {
      node_type = Node::GROUP;
      logical_type = LogicalType::LIST;
    }
    if (node_type == Node::GROUP) {
      schema = Local<Object>::Cast(Nan::Get(value, Nan::New("schema").ToLocalChecked()).ToLocalChecked());
      fields.push_back(SetupSchema(key_str, repetition, schema));
    } else {
      fields.push_back(PrimitiveNode::Make(key_str, repetition, parquet_type, logical_type));
    }
  }

  return GroupNode::Make(root_name, root_repetition, fields);
}

Nan::Persistent<Function> ParquetWriter::constructor;

ParquetWriter::ParquetWriter(const Nan::FunctionCallbackInfo<Value>& info) : pw_(nullptr), fw_(nullptr), ncols_(0) {
  // Arguments sanity checks
  if (info.Length() < 2) {
    Nan::ThrowTypeError("Wrong number of arguments");
    return;
  }
  if (!info[1]->IsObject()) {
    Nan::ThrowTypeError("second argument is not an object");
    return;
  }
  String::Utf8Value param1(v8::Isolate::GetCurrent(), info[0]->ToString(Nan::GetCurrentContext()).FromMaybe(v8::Local<v8::String>()));
  String::Utf8Value param3(v8::Isolate::GetCurrent(), info[2]->ToString(Nan::GetCurrentContext()).FromMaybe(v8::Local<v8::String>()));
  Local<Object> param2 = Local<Object>::Cast(info[1]);
  std::shared_ptr<GroupNode> schema = std::static_pointer_cast<GroupNode>(SetupSchema("schema", Repetition::REQUIRED, param2));
  arrow::Status status = arrow::io::FileOutputStream::Open(std::string(*param1), &fw_);
  ncols_ = param2->GetOwnPropertyNames(Nan::GetCurrentContext()).ToLocalChecked()->Length();
  Compression::type compression;
  std::string comp_str(*param3);
  if (comp_str.compare("snappy") == 0) {
    compression = Compression::SNAPPY;
  } else if (comp_str.compare("gzip") == 0) {
    compression = Compression::GZIP;
  } else if (comp_str.compare("lzo") == 0) {
    compression = Compression::LZO;
  } else if (comp_str.compare("brotli") == 0) {
    compression = Compression::BROTLI;
  } else if (comp_str.compare("undefined") == 0) {
    compression = Compression::UNCOMPRESSED;
  } else {
    Nan::ThrowTypeError("Wrong compression type");
    return;
  }
  std::shared_ptr<WriterProperties> writer_properties = WriterProperties::Builder().compression(compression)->build();

  if (!status.ok()) {
    std::stringstream ss;
    ss << "Parquet/Arrow error: " << status.ToString();
    Nan::ThrowError(Nan::New(ss.str()).ToLocalChecked());
    return;
  }
  try {
    pw_ = parquet::ParquetFileWriter::Open(fw_, schema, writer_properties);
  } catch (const std::exception& e) {
    Nan::ThrowError(Nan::New(e.what()).ToLocalChecked());
  }
}

ParquetWriter::~ParquetWriter() {}

void ParquetWriter::Init(Local<Object> exports) {
  Nan::HandleScope scope;

  Local<FunctionTemplate> tpl = Nan::New<FunctionTemplate>(New);

  tpl->SetClassName(Nan::New("ParquetWriter").ToLocalChecked());
  tpl->InstanceTemplate()->SetInternalFieldCount(1);

  Nan::SetPrototypeMethod(tpl, "write", Write);
  Nan::SetPrototypeMethod(tpl, "close", Close);

  constructor.Reset(Nan::GetFunction(tpl).ToLocalChecked());
  Nan::Set(exports, Nan::New("ParquetWriter").ToLocalChecked(), Nan::GetFunction(tpl).ToLocalChecked());
}

void ParquetWriter::New(const Nan::FunctionCallbackInfo<Value>& info) {
  ParquetWriter* obj = new ParquetWriter(info);
  obj->Wrap(info.This());
  info.GetReturnValue().Set(info.This());
}

void ParquetWriter::NewInstance(const Nan::FunctionCallbackInfo<Value>& info) {
  const unsigned argc = 2;
  Local<Value> argv[argc] = { info[0], info[1] };
  Local<Function> cons = Nan::New<Function>(constructor);
  info.GetReturnValue().Set(Nan::NewInstance(cons, argc, argv).ToLocalChecked());
}

void ParquetWriter::Close(const Nan::FunctionCallbackInfo<Value>& info) {
  ParquetWriter* obj = ObjectWrap::Unwrap<ParquetWriter>(info.Holder());
  try {
    obj->pw_->Close();
    arrow::Status status = obj->fw_->Close();
  } catch (const std::exception& e) {
    Nan::ThrowError(Nan::New(e.what()).ToLocalChecked());
  }
}

static void write_bool(parquet::ColumnWriter* column_writer, Local<Value> val, int16_t* def, int16_t* rep, bool is_required) {
  parquet::BoolWriter* writer = static_cast<parquet::BoolWriter*>(column_writer);
  bool input_value;
  bool* value = &input_value;
  int16_t zerodef = 0;
  int16_t* cdef = def;

  if (val->IsUndefined()) {
    cdef = &zerodef;
    value = nullptr;
  } else {
    input_value = val->ToBoolean(Nan::GetCurrentContext()).ToLocalChecked()->Value();
  }
  writer->WriteBatch(1, cdef, rep, value);
}

static void write_int32(parquet::ColumnWriter* column_writer, Local<Value> val, int16_t* def, int16_t* rep, bool is_required) {
  parquet::Int32Writer* writer = static_cast<parquet::Int32Writer*>(column_writer);
  int32_t input_value;
  int32_t* value = &input_value;
  int16_t zerodef = 0;
  int16_t* cdef = def;

  if (val->IsUndefined()) {
    cdef = &zerodef;
    value = nullptr;
  } else {
    input_value = val->ToInt32(Nan::GetCurrentContext()).ToLocalChecked()->Value();
  }
  writer->WriteBatch(1, cdef, rep, value);
}

static void write_int64(parquet::ColumnWriter* column_writer, Local<Value> val, int16_t* def, int16_t* rep, bool is_required) {
  parquet::Int64Writer* writer = static_cast<parquet::Int64Writer*>(column_writer);
  int64_t input_value;
  int64_t* value = &input_value;
  int16_t zerodef = 0;
  int16_t* cdef = def;

  if (val->IsUndefined()) {
    cdef = &zerodef;
    value = nullptr;
  } else {
    input_value = val->ToInteger(Nan::GetCurrentContext()).ToLocalChecked()->Value();
  }
  writer->WriteBatch(1, cdef, rep, value);
}

static void write_int96(parquet::ColumnWriter* column_writer, Local<Value> val, int16_t* def, int16_t* rep, bool is_required) {
  parquet::Int96Writer* writer = static_cast<parquet::Int96Writer*>(column_writer);
  parquet::Int96 input_value;
  parquet::Int96* value = &input_value;
  int16_t zerodef = 0;
  int16_t* cdef = def;

  if (val->IsUndefined()) {
    cdef = &zerodef;
    value = nullptr;
  } else {
    Local<Object> obj_value = Local<Object>::Cast(val);
    uint32_t* buf = (uint32_t*) node::Buffer::Data(obj_value);
    input_value.value[0] = buf[0];
    input_value.value[1] = buf[1];
    input_value.value[2] = buf[2];
  }
  writer->WriteBatch(1, cdef, rep, value);
}

static void write_float(parquet::ColumnWriter* column_writer, Local<Value> val, int16_t* def, int16_t* rep, bool is_required) {
  parquet::FloatWriter* writer = static_cast<parquet::FloatWriter*>(column_writer);
  float input_value;
  float* value = &input_value;
  int16_t zerodef = 0;
  int16_t* cdef = def;

  if (val->IsUndefined()) {
    cdef = &zerodef;
    value = nullptr;
  } else {
    input_value = val->ToNumber(Nan::GetCurrentContext()).ToLocalChecked()->Value();
  }
  writer->WriteBatch(1, cdef, rep, value);
}

static void write_double(parquet::ColumnWriter* column_writer, Local<Value> val, int16_t* def, int16_t* rep, bool is_required) {
  parquet::DoubleWriter* writer = static_cast<parquet::DoubleWriter*>(column_writer);
  double input_value;
  double* value = &input_value;
  int16_t zerodef = 0;
  int16_t* cdef = def;

  if (val->IsUndefined()) {
    cdef = &zerodef;
    value = nullptr;
  } else {
    input_value = val->ToNumber(Nan::GetCurrentContext()).ToLocalChecked()->Value();
  }
  writer->WriteBatch(1, cdef, rep, value);
}

static void write_byte_array(parquet::ColumnWriter* column_writer, Local<Value> val, int16_t* def, int16_t* rep, bool is_required) {
  parquet::ByteArrayWriter* writer = static_cast<parquet::ByteArrayWriter*>(column_writer);
  parquet::ByteArray input_value;
  parquet::ByteArray* value = &input_value;
  int16_t zerodef = 0;
  int16_t* cdef = def;

  if (val->IsString()) {
    String::Utf8Value val_utf8(v8::Isolate::GetCurrent(), val->ToString(Nan::GetCurrentContext()).FromMaybe(v8::Local<v8::String>()));
    std::string str = std::string(*val_utf8);
    Local<Object> buf = Nan::CopyBuffer(str.data(), str.length()).ToLocalChecked();
    input_value.ptr = reinterpret_cast<const uint8_t*>(node::Buffer::Data(buf));
    input_value.len = node::Buffer::Length(buf);
  } else if (node::Buffer::HasInstance(val)) {
    Local<Object> obj_value(val->ToObject(Nan::GetCurrentContext()).ToLocalChecked());
    input_value.ptr = reinterpret_cast<const uint8_t*>(node::Buffer::Data(obj_value));
    input_value.len = node::Buffer::Length(obj_value);
  } else if (val->IsUndefined()) {
    if (is_required)
      return Nan::ThrowTypeError(Nan::New("a byte array value is required").ToLocalChecked());
    cdef = &zerodef;
    value = nullptr;
  } else {
    Nan::ThrowTypeError(Nan::New("Parameter is not a byte array").ToLocalChecked());
  }

  writer->WriteBatch(1, cdef, rep, value);
}

static void write_flba(parquet::ColumnWriter* column_writer, Local<Value> val, int16_t* def, int16_t* rep, bool is_required) {
  parquet::FixedLenByteArrayWriter* writer = static_cast<parquet::FixedLenByteArrayWriter*>(column_writer);
  parquet::FixedLenByteArray input_value;
  parquet::FixedLenByteArray* value = &input_value;
  int16_t zerodef = 0;
  int16_t* cdef = def;

  if (val->IsUndefined()) {
    cdef = &zerodef;
    value = nullptr;
  } else {
    Local<Object> obj_value = Local<Object>::Cast(val);
    input_value.ptr = reinterpret_cast<const uint8_t*>(node::Buffer::Data(obj_value));
  }
  writer->WriteBatch(1, cdef, rep, value);
}

typedef void (*writer_t)(parquet::ColumnWriter*, Local<Value>, int16_t*, int16_t*, bool);

// Table of writer functions, keep same ordering as parquet::Type
static writer_t type_writers[] = {
  write_bool,
  write_int32,
  write_int64,
  write_int96,
  write_float,
  write_double,
  write_byte_array,
  write_flba
};

void ParquetWriter::Write(const Nan::FunctionCallbackInfo<Value>& info) {
  ParquetWriter* obj = ObjectWrap::Unwrap<ParquetWriter>(info.Holder());

  if (!info[0]->IsArray()) {
    Nan::ThrowTypeError(Nan::New("Parameter is not an array").ToLocalChecked());
    return;
  }
  Local<Array> input = Local<Array>::Cast(info[0]);
  int num_rows = input->Length();
  try {
    parquet::RowGroupWriter* rgw = obj->pw_->AppendRowGroup(num_rows);

    for (int i = 0; i < obj->ncols_; i++) {
      parquet::ColumnWriter *column_writer = rgw->NextColumn();
      const parquet::ColumnDescriptor *descr = column_writer->descr();
      int16_t maxdef = descr->max_definition_level();
      int16_t maxrep = descr->max_repetition_level();
      int16_t zerorep = 0;
      bool is_required = descr->schema_node()->is_required();
      Local<Object> row;
      Local<Value> val;
      writer_t type_writer = type_writers[column_writer->type()];

      for (int j = 0; j < num_rows; j++) {
        row = Local<Array>::Cast(Nan::Get(input, j).ToLocalChecked());
        val = Nan::Get(row, i).ToLocalChecked();
        if (val->IsArray()) {
          Local<Array> array = Local<Array>::Cast(val);
          int len = array->Length();
          type_writer(column_writer, Nan::Get(array, 0).ToLocalChecked(), &maxdef, &zerorep, is_required);
          for (int k = 1; k < len; k++) {
            type_writer(column_writer, Nan::Get(array, k).ToLocalChecked(), &maxdef, &maxrep, is_required);
          }
        } else {
            type_writer(column_writer, val, &maxdef, nullptr, is_required);
        }
      }
    }
  } catch (const std::exception& e) {
    return Nan::ThrowError(Nan::New(e.what()).ToLocalChecked());
  }
  info.GetReturnValue().Set(Nan::New<Number>(num_rows));
}
