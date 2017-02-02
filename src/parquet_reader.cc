#include <iostream>

#include "parquet_reader.h"

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
  String::Utf8Value param1(info[0]->ToString());
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
  Nan::SetPrototypeMethod(tpl, "readline", Readline);

  constructor.Reset(tpl->GetFunction());
  exports->Set(Nan::New("ParquetReader").ToLocalChecked(), tpl->GetFunction());
}

void ParquetReader::New(const Nan::FunctionCallbackInfo<Value>& info) {
  ParquetReader* obj = new ParquetReader(info);
  obj->Wrap(info.This());

  info.GetReturnValue().Set(info.This());
}

void ParquetReader::NewInstance(const Nan::FunctionCallbackInfo<Value>& info) {
  const unsigned argc = 1;
  Local<Value> argv[argc] = { info[0] };
  Local<Function> cons = Nan::New<v8::Function>(constructor);
  info.GetReturnValue().Set(cons->NewInstance(argc, argv));
}

void ParquetReader::DebugPrint(const Nan::FunctionCallbackInfo<Value>& info) {
  ParquetReader* obj = ObjectWrap::Unwrap<ParquetReader>(info.Holder());
  std::list<int> columns;
  bool print_values = true;

  obj->pr_->DebugPrint(std::cout, columns, print_values);
  info.GetReturnValue().Set(Nan::New("Hello").ToLocalChecked());
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
  res->Set(Nan::New<Number>(2) , Nan::New<Number>(file_metadata->num_rows()));

  info.GetReturnValue().Set(res);
}

void ParquetReader::ReadSync(const Nan::FunctionCallbackInfo<Value>& info) {
  int colnum;
  int start;
  int n = 10;
  int rows_read;
  int64_t values_read;
  ParquetReader* obj = ObjectWrap::Unwrap<ParquetReader>(info.Holder());
  std::shared_ptr<parquet::FileMetaData> file_metadata = obj->pr_->metadata();
  Local<Object> res = Nan::New<Object>();
  Nan::MaybeLocal<Object> buffer = Nan::NewBuffer(10 * 4);
  std::shared_ptr<parquet::RowGroupReader> row_group_reader = obj->pr_->RowGroup(0);
  std::shared_ptr<parquet::ColumnReader> column_reader = row_group_reader->Column(1);
  parquet::Int32Reader* int32_reader = static_cast<parquet::Int32Reader*>(column_reader.get());
  int32_t* result = new int32_t[n];

  rows_read = int32_reader->ReadBatch(10, nullptr, nullptr, result, &values_read);
  std::cout << "rows_read: " << rows_read << std::endl;
  std::cout << "values_read: " << values_read << std::endl;
  std::cout << "result[9]: " << result[9] << std::endl;
  if (info.Length() < 3) {
    Nan::ThrowTypeError("wrong number of arguments");
    return;
  }
  if (!info[0]->IsNumber() || !info[1]->IsNumber() || !info[2]->IsNumber()) {
    Nan::ThrowTypeError("wrong argument");
    return;
  }
  colnum = info[0]->IntegerValue();
  start = info[1]->IntegerValue();
  n = info[2]->IntegerValue();
  std::cout << "colnum: " << colnum << std::endl;
  std::cout << "start: " << start << std::endl;
  std::cout << "n: " << n << std::endl;
  std::cout << "sizeof(bool): " << sizeof(bool) << std::endl;

  const parquet::ColumnDescriptor* descr = file_metadata->schema()->Column(2);
  std::cout << "type 1: " << descr->physical_type() << std::endl;
  info.GetReturnValue().Set(res);
}

void ParquetReader::Readline(const Nan::FunctionCallbackInfo<Value>& info) {
  ParquetReader* obj = ObjectWrap::Unwrap<ParquetReader>(info.Holder());
  std::shared_ptr<parquet::FileMetaData> file_metadata = obj->pr_->metadata();
  Local<Object> res = Nan::New<Object>();
  std::shared_ptr<parquet::RowGroupReader> row_group_reader = obj->pr_->RowGroup(0);
  int num_columns = file_metadata->num_columns();

  if (!info[0]->IsNumber() || !info[1]->IsNumber()) {
    Nan::ThrowTypeError("wrong argument");
    return;
  }
  int nskip = info[0]->IntegerValue();
  int nrows = info[1]->IntegerValue();

  for (int l = 0; l < nrows; l++) {
    Local<Object> row_res = Nan::New<Object>();

    res->Set(Nan::New<Number>(nskip + l), row_res);
    for (int i = 0; i < num_columns; i++) {
      int64_t values_read;
      std::shared_ptr<parquet::ColumnReader> column_reader = row_group_reader->Column(i);

      //std::cout << "i: " << i << ", type: " << column_reader->type() << std::endl;
      switch (column_reader->type()) {
        case parquet::Type::BOOLEAN: {
          bool value;
          parquet::BoolReader* reader = static_cast<parquet::BoolReader*>(column_reader.get());
          reader->Skip(nskip + l);
          reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
          row_res->Set(Nan::New<Number>(i), Nan::New<Boolean>(value));
          break;
        }
        case parquet::Type::INT32: {
          int32_t value;
          parquet::Int32Reader* reader = static_cast<parquet::Int32Reader*>(column_reader.get());
          reader->Skip(nskip + l);
          reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
          row_res->Set(Nan::New<Number>(i), Nan::New<Number>(value));
          break;
        }
        case parquet::Type::INT64: {
          int64_t value;
          parquet::Int64Reader* reader = static_cast<parquet::Int64Reader*>(column_reader.get());
          reader->Skip(nskip + l);
          reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
          row_res->Set(Nan::New<Number>(i), Nan::New<Number>(value));
          break;
        }
        case parquet::Type::INT96: {
          parquet::Int96 value;
          parquet::Int96Reader* reader = static_cast<parquet::Int96Reader*>(column_reader.get());
          reader->Skip(nskip + l);
          reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
          row_res->Set(Nan::New<Number>(i), Nan::CopyBuffer((char*)value.value, 12).ToLocalChecked());
          break;
        }
        case parquet::Type::FLOAT: {
          float value;
          parquet::FloatReader* reader = static_cast<parquet::FloatReader*>(column_reader.get());
          reader->Skip(nskip + l);
          reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
          row_res->Set(Nan::New<Number>(i), Nan::New<Number>(value));
          break;
        }
        case parquet::Type::DOUBLE: {
          double value;
          parquet::DoubleReader* reader = static_cast<parquet::DoubleReader*>(column_reader.get());
          reader->Skip(nskip + l);
          reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
          row_res->Set(Nan::New<Number>(i), Nan::New<Number>(value));
          break;
        }
        case parquet::Type::BYTE_ARRAY: {
          parquet::ByteArray value;
          parquet::ByteArrayReader* reader = static_cast<parquet::ByteArrayReader*>(column_reader.get());
          reader->Skip(2);
          reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
          row_res->Set(Nan::New<Number>(i), Nan::CopyBuffer((char*)value.ptr, value.len).ToLocalChecked());
          break;
        }
        case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
          parquet::FixedLenByteArray value;
          parquet::FixedLenByteArrayReader* reader = static_cast<parquet::FixedLenByteArrayReader*>(column_reader.get());
          reader->Skip(nskip + l);
          reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
          row_res->Set(Nan::New<Number>(i), Nan::CopyBuffer((char*)value.ptr, 1).ToLocalChecked());
          break;
        }
      }
    }
  }

  info.GetReturnValue().Set(res);
}
