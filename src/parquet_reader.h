#ifndef PARQUET_READER_H
#define PARQUET_READER_H

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
    static void ReadColumn(const Nan::FunctionCallbackInfo<v8::Value>& args);
    static void Close(const Nan::FunctionCallbackInfo<v8::Value>& args);
    static Nan::Persistent<v8::Function> constructor;

    // Wrapped object
    std::unique_ptr<parquet::ParquetFileReader> pr_;
};

#endif
