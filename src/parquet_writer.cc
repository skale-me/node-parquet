#include <iostream>

#include <arrow/io/file.h>

#include "parquet_writer.h"

using parquet::LogicalType;
using parquet::Repetition;
using parquet::Type;
using parquet::schema::PrimitiveNode;
using parquet::schema::GroupNode;

using v8::Array;
using v8::Function;
using v8::FunctionTemplate;
using v8::Local;
using v8::Int32;
using v8::Number;
using v8::Object;
using v8::String;
using v8::Value;

static std::shared_ptr<GroupNode> SetupSchema(Local<Object> obj) {
  parquet::schema::NodeVector fields;
  Local<Array> properties = obj->GetOwnPropertyNames();
  int len = properties->Length();

  for (int i = 0; i < len; i++) {
    Local<Value> key = properties->Get(i);
    Local<Object> value = Local<Object>::Cast(obj->Get(key));
    String::Utf8Value key_utf8(key->ToString());
    Local<Value> type = value->Get(Nan::New("type").ToLocalChecked());
    Local<Value> optional = value->Get(Nan::New("optional").ToLocalChecked());
    Local<Value> repeat = value->Get(Nan::New("repeat").ToLocalChecked());
    String::Utf8Value type_utf8(type->ToString());
    std::string type_str = std::string(*type_utf8);
    std::string key_str = std::string(*key_utf8);
    Type::type parquet_type = Type::BOOLEAN;
    Repetition::type repetition;

    std::cout << "key: " << key_str << std::endl;
    std::cout << "type: " << type_str << std::endl;

    if (optional->BooleanValue()) {
      repetition = Repetition::OPTIONAL;
    } else {
      if (repeat->BooleanValue()) {
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
    } else if (type_str.compare("int96") == 0) {
      parquet_type = Type::INT96;
    } else if (type_str.compare("float") == 0) {
      parquet_type = Type::FLOAT;
    } else if (type_str.compare("double") == 0) {
      parquet_type = Type::DOUBLE;
    } else if (type_str.compare("byte_array") == 0) {
      parquet_type = Type::BYTE_ARRAY;
    } else if (type_str.compare("fixed_len_byte_array") == 0) {
      parquet_type = Type::FIXED_LEN_BYTE_ARRAY;
    }
    fields.push_back(PrimitiveNode::Make(key_str, repetition, parquet_type, LogicalType::NONE));
  }
//  std::cout << "schema length: " << properties->Length() << std::endl;

  return std::static_pointer_cast<GroupNode>(
    GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

Nan::Persistent<Function> ParquetWriter::constructor;

ParquetWriter::ParquetWriter(const Nan::FunctionCallbackInfo<Value>& info) : pw_(nullptr), fw_(nullptr), ncols_(0) {
  String::Utf8Value param1(info[0]->ToString());
  std::string to = std::string(*param1);
  Local<Object> param2 = Local<Object>::Cast(info[1]);
  std::shared_ptr<GroupNode> schema = SetupSchema(param2);
  arrow::Status status = arrow::io::FileOutputStream::Open(to, &fw_);
  ncols_ = param2->GetOwnPropertyNames()->Length();

  if (!status.ok()) {
    std::stringstream ss;
    ss << "Parquet/Arrow error: " << status.ToString();
    Nan::ThrowTypeError(Nan::New(ss.str()).ToLocalChecked());
    return;
  }
  pw_ = parquet::ParquetFileWriter::Open(fw_, schema);
}

ParquetWriter::~ParquetWriter() {}

void ParquetWriter::Init(Local<Object> exports) {
  Nan::HandleScope scope;

  Local<FunctionTemplate> tpl = Nan::New<FunctionTemplate>(New);

  tpl->SetClassName(Nan::New("ParquetWriter").ToLocalChecked());
  tpl->InstanceTemplate()->SetInternalFieldCount(1);

  Nan::SetPrototypeMethod(tpl, "writeSync", WriteSync);
  Nan::SetPrototypeMethod(tpl, "close", Close);

  constructor.Reset(tpl->GetFunction());
  exports->Set(Nan::New("ParquetWriter").ToLocalChecked(), tpl->GetFunction());
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
  info.GetReturnValue().Set(cons->NewInstance(argc, argv));
}

void ParquetWriter::Close(const Nan::FunctionCallbackInfo<Value>& info) {
  ParquetWriter* obj = ObjectWrap::Unwrap<ParquetWriter>(info.Holder());
  obj->pw_->Close();
  obj->fw_->Close();
}

void ParquetWriter::WriteSync(const Nan::FunctionCallbackInfo<Value>& info) {
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
      int16_t definition_level = descr->max_definition_level();
      int16_t* definition = definition_level ? &definition_level : nullptr;
      Local<Object> row;
      Local<Value> val;

      // FIXME: Handle repetition levels

      switch (column_writer->type()) {
        case parquet::Type::BOOLEAN: {
          parquet::BoolWriter* writer = static_cast<parquet::BoolWriter*>(column_writer);
          bool input_value, *value;
          for (int j = 0; j < num_rows; j++) {
            row = Local<Array>::Cast(input->Get(j));
            val = row->Get(i);
            if (val->IsUndefined()) {
              definition_level = 0;
              value = nullptr;
            } else {
              input_value = val->BooleanValue();
              value = &input_value;
            }
            writer->WriteBatch(1, definition, nullptr, value);
          }
          break;
        }
        case parquet::Type::INT32: {
          parquet::Int32Writer* writer = static_cast<parquet::Int32Writer*>(column_writer);
          int32_t input_value, *value;
          for (int j = 0; j < num_rows; j++) {
            row = Local<Array>::Cast(input->Get(j));
            val = row->Get(i);
            if (val->IsUndefined()) {
              definition_level = 0;
              value = nullptr;
            } else {
              input_value = val->Int32Value();
              value = &input_value;
            }
            writer->WriteBatch(1, definition, nullptr, value);
          }
          break;
        }
        case parquet::Type::INT64: {
          parquet::Int64Writer* writer = static_cast<parquet::Int64Writer*>(column_writer);
          int64_t input_value, *value;
          for (int j = 0; j < num_rows; j++) {
            row = Local<Array>::Cast(input->Get(j));
            val = row->Get(i);
            if (val->IsUndefined()) {
              definition_level = 0;
              value = nullptr;
            } else {
              input_value = row->Get(i)->IntegerValue();
              value = &input_value;
            }
            writer->WriteBatch(1, definition, nullptr, value);
          }
          break;
        }
        case parquet::Type::INT96: {
          parquet::Int96Writer* writer = static_cast<parquet::Int96Writer*>(column_writer);
          parquet::Int96 input_value, *value;
          for (int j = 0; j < num_rows; j++) {
            row = Local<Array>::Cast(input->Get(j));
            val = row->Get(i);
            if (val->IsUndefined()) {
              definition_level = 0;
              value = nullptr;
            } else {
              Local<Object> obj_value = Local<Object>::Cast(val);
              uint32_t *buf = (uint32_t*) node::Buffer::Data(obj_value);
              input_value.value[0] = buf[0];
              input_value.value[1] = buf[1];
              input_value.value[2] = buf[2];
              value = &input_value;
            }
            writer->WriteBatch(1, definition, nullptr, value);
          }
          break;
        }
        case parquet::Type::FLOAT: {
          parquet::FloatWriter* writer = static_cast<parquet::FloatWriter*>(column_writer);
          float input_value, *value;
          for (int j = 0; j < num_rows; j++) {
            row = Local<Array>::Cast(input->Get(j));
            val = row->Get(i);
            if (val->IsUndefined()) {
              definition_level = 0;
              value = nullptr;
            } else {
              input_value = (float) val->NumberValue();
              value = &input_value;
            }
            writer->WriteBatch(1, definition, nullptr, value);
          }
          break;
        }
        case parquet::Type::DOUBLE: {
          parquet::DoubleWriter* writer = static_cast<parquet::DoubleWriter*>(column_writer);
          double input_value, *value;
          for (int j = 0; j < num_rows; j++) {
            row = Local<Array>::Cast(input->Get(j));
            val = row->Get(i);
            if (row->Get(i)->IsUndefined()) {
              definition_level = 0;
              value = nullptr;
            } else {
              input_value = val->NumberValue();
              value = &input_value;
            }
            writer->WriteBatch(1, definition, nullptr, value);
          }
          break;
        }
        case parquet::Type::BYTE_ARRAY: {
          parquet::ByteArrayWriter* writer = static_cast<parquet::ByteArrayWriter*>(column_writer);
          parquet::ByteArray input_value, *value;
          for (int j = 0; j < num_rows; j++) {
            row = Local<Array>::Cast(input->Get(j));
            val = row->Get(i);
            if (val->IsUndefined()) {
              definition_level = 0;
              value = nullptr;
            } else if (val->IsString()) {
              String::Utf8Value val_utf8(val->ToString());
              input_value.ptr = reinterpret_cast<const uint8_t*>(std::string(*val_utf8).c_str());
              input_value.len = val_utf8.length();
              value = &input_value;
            } else if (val->IsObject()) {
              Local<Object> obj_value = Local<Object>::Cast(val);
              input_value.ptr = reinterpret_cast<const uint8_t*>(node::Buffer::Data(obj_value));
              input_value.len = node::Buffer::Length(obj_value);
              value = &input_value;
            }
            writer->WriteBatch(1, definition, nullptr, value);
          }
          break;
        }
        case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
          parquet::FixedLenByteArrayWriter* writer = static_cast<parquet::FixedLenByteArrayWriter*>(column_writer);
          parquet::FixedLenByteArray input_value, *value;
          for (int j = 0; j < num_rows; j++) {
            row = Local<Array>::Cast(input->Get(j));
            val = row->Get(i);
            if (row->Get(i)->IsUndefined()) {
              definition_level = 0;
              value = nullptr;
            } else {
              Local<Object> obj_value = Local<Object>::Cast(val);
              input_value.ptr = reinterpret_cast<const uint8_t*>(node::Buffer::Data(obj_value));
              value = &input_value;
            }
            writer->WriteBatch(1, definition, nullptr, value);
          }
          break;
        }
      }
    }
  } catch (const std::exception& e) {
    Nan::ThrowError(Nan::New(e.what()).ToLocalChecked());
    return;
  }
  info.GetReturnValue().Set(Nan::New<Number>(num_rows));
}
