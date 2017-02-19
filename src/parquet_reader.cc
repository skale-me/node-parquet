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

ParquetReader::ParquetReader(const Nan::FunctionCallbackInfo<Value>& info) : pr_(nullptr) {
  if (!info[0]->IsString()) {
    Nan::ThrowTypeError("wrong argument");
    return;
  }
  String::Utf8Value param1(info[0]->ToString());
  std::string from = std::string(*param1);

  try {
    pr_ = parquet::ParquetFileReader::OpenFile(from);
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
  Nan::SetPrototypeMethod(tpl, "readColumn", ReadColumn);
  Nan::SetPrototypeMethod(tpl, "close", Close);

  constructor.Reset(tpl->GetFunction());
  exports->Set(Nan::New("ParquetReader").ToLocalChecked(), tpl->GetFunction());
}

void ParquetReader::New(const Nan::FunctionCallbackInfo<Value>& info) {
  if (info.IsConstructCall()) {
    ParquetReader* obj = new ParquetReader(info);
    obj->Wrap(info.This());
    info.GetReturnValue().Set(info.This());
  } else {
    const int argc = 1;
    Local<Value> argv[argc] = { info[0] };
    Local<Function> cons = Nan::New<v8::Function>(constructor);
    info.GetReturnValue().Set(cons->NewInstance(argc, argv));
  }
}

void ParquetReader::NewInstance(const Nan::FunctionCallbackInfo<Value>& info) {
  const int argc = 1;
  Local<Value> argv[argc] = { info[0] };
  Local<Function> cons = Nan::New<v8::Function>(constructor);
  info.GetReturnValue().Set(cons->NewInstance(argc, argv));
}

void ParquetReader::Info(const Nan::FunctionCallbackInfo<Value>& info) {
  ParquetReader* obj = ObjectWrap::Unwrap<ParquetReader>(info.Holder());
  std::shared_ptr<parquet::FileMetaData> file_metadata = obj->pr_->metadata();
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
  obj->pr_->Close();
}

void ParquetReader::ReadColumn(const Nan::FunctionCallbackInfo<Value>& info) {
  ParquetReader* obj = ObjectWrap::Unwrap<ParquetReader>(info.Holder());
  std::shared_ptr<parquet::FileMetaData> file_metadata = obj->pr_->metadata();
  Local<Array> res = Nan::New<Array>();
  std::shared_ptr<parquet::RowGroupReader> row_group_reader = obj->pr_->RowGroup(0);

  if (!info[0]->IsNumber() || !info[1]->IsNumber() || !info[2]->IsNumber()) {
    Nan::ThrowTypeError("wrong argument");
    return;
  }
  int col = info[0]->IntegerValue();
  int nskip = info[1]->IntegerValue();
  int nrows = info[2]->IntegerValue();
  try {
    std::shared_ptr<parquet::ColumnReader> column_reader = row_group_reader->Column(col);
    parquet::LogicalType::type logical_type = column_reader->descr()->logical_type();
    int64_t values_read;
    int16_t definitions[nrows];
    int16_t repetitions[nrows];
    int i = 0, j = 0;

    switch (column_reader->type()) {
      case parquet::Type::BOOLEAN: {
        bool values[nrows];
        parquet::BoolReader* reader = static_cast<parquet::BoolReader*>(column_reader.get());
        if (nskip)
          reader->Skip(nskip);
        reader->ReadBatch(nrows, definitions, repetitions, values, &values_read);
        for (i = 0; i < nrows; i++)
          if (definitions[i])
            res->Set(Nan::New<Number>(i), Nan::New<Boolean>(values[j++]));
        break;
      }
      case parquet::Type::INT32: {
        int32_t values[nrows];
        parquet::Int32Reader* reader = static_cast<parquet::Int32Reader*>(column_reader.get());
        if (nskip)
          reader->Skip(nskip);
        reader->ReadBatch(nrows, definitions, repetitions, values, &values_read);
        for (i = 0; i < nrows; i++)
          if (definitions[i])
            res->Set(Nan::New<Number>(i), Nan::New<Number>(values[j++]));
        break;
      }
      case parquet::Type::INT64: {
        int64_t values[nrows];
        parquet::Int64Reader* reader = static_cast<parquet::Int64Reader*>(column_reader.get());
        if (nskip)
          reader->Skip(nskip);
        reader->ReadBatch(nrows, definitions, repetitions, values, &values_read);
        if (col == 1)
          std::cout << "def: " << definitions[i] << ", rep: " << repetitions[i] <<  std::endl;
        for (i = 0; i < nrows; i++)
          if (definitions[i])
            res->Set(Nan::New<Number>(i), Nan::New<Number>(values[j++]));
        break;
      }
      case parquet::Type::INT96: {
        parquet::Int96 values[nrows];
        parquet::Int96Reader* reader = static_cast<parquet::Int96Reader*>(column_reader.get());
        if (nskip)
          reader->Skip(nskip);
        reader->ReadBatch(nrows, definitions, repetitions, values, &values_read);
        for (i = 0; i < nrows; i++)
          if (definitions[i])
            res->Set(Nan::New<Number>(i), Nan::CopyBuffer((char*)values[j++].value, 12).ToLocalChecked());
        break;
      }
      case parquet::Type::FLOAT: {
        float values[nrows];
        parquet::FloatReader* reader = static_cast<parquet::FloatReader*>(column_reader.get());
        if (nskip)
          reader->Skip(nskip);
        reader->ReadBatch(nrows, definitions, repetitions, values, &values_read);
        for (i = 0; i < nrows; i++)
          if (definitions[i])
            res->Set(Nan::New<Number>(i), Nan::New<Number>(values[j++]));
        break;
      }
      case parquet::Type::DOUBLE: {
        double values[nrows];
        parquet::DoubleReader* reader = static_cast<parquet::DoubleReader*>(column_reader.get());
        if (nskip)
          reader->Skip(nskip);
        reader->ReadBatch(nrows, definitions, repetitions, values, &values_read);
        for (i = 0; i < nrows; i++)
          if (definitions[i])
            res->Set(Nan::New<Number>(i), Nan::New<Number>(values[j++]));
        break;
      }
      case parquet::Type::BYTE_ARRAY: {
        std::vector<parquet::ByteArray> values(nrows);
        parquet::ByteArrayReader* reader = static_cast<parquet::ByteArrayReader*>(column_reader.get());
        if (nskip)
          reader->Skip(nskip);
        reader->ReadBatch(nrows, definitions, repetitions, &values[0], &values_read);
        for (i = 0; i < nrows; i++)
          if (definitions[i]) {
            if (logical_type == parquet::LogicalType::UTF8) {
              if (col > 6) {
                std::cout << "def: " << definitions[i] << ", rep: " << repetitions[i] <<  std::endl;
              }
              res->Set(Nan::New<Number>(i), Nan::New((char*)values[j].ptr, values[j].len).ToLocalChecked());
            } else {
              res->Set(Nan::New<Number>(i), Nan::CopyBuffer((char*)values[j].ptr, values[j].len).ToLocalChecked());
            }
            j++;
          }
        break;
      }
      case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
        std::vector<parquet::FixedLenByteArray> values(nrows);
        parquet::FixedLenByteArrayReader* reader = static_cast<parquet::FixedLenByteArrayReader*>(column_reader.get());
        if (nskip)
          reader->Skip(nskip);
        reader->ReadBatch(nrows, definitions, repetitions, &values[0], &values_read);
        for (i = 0; i < nrows; i++)
          if (definitions[i])
            res->Set(Nan::New<Number>(i), Nan::CopyBuffer((char*)values[j++].ptr, 1).ToLocalChecked());
        break;
      }
    }
  } catch (const std::exception& e) {
    Nan::ThrowError(Nan::New(e.what()).ToLocalChecked());
    return;
  }
  info.GetReturnValue().Set(res);
}
