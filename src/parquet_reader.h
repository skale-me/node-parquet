// Copyright 2017 Luca-SAS, licensed under the Apache License 2.0

#ifndef PARQUET_READER_H
#define PARQUET_READER_H

#include <vector>

#include <nan.h>

#include <parquet/api/reader.h>

class ParquetReader : public Nan::ObjectWrap {
  public:
    static void Init(v8::Local<v8::Object> exports);

  private:
    ParquetReader(const Nan::FunctionCallbackInfo<v8::Value>& args);
    ~ParquetReader();

    static void NewInstance(const Nan::FunctionCallbackInfo<v8::Value>& args);
    static void New(const Nan::FunctionCallbackInfo<v8::Value>& args);
    static void Info(const Nan::FunctionCallbackInfo<v8::Value>& args);
    static void Read(const Nan::FunctionCallbackInfo<v8::Value>& args);
    static void Close(const Nan::FunctionCallbackInfo<v8::Value>& args);
    static Nan::Persistent<v8::Function> constructor;

    // Wrapped objects
    std::unique_ptr<parquet::ParquetFileReader> parquet_file_reader_;
    std::vector<std::shared_ptr<parquet::ColumnReader>> column_readers_;
};

#endif
