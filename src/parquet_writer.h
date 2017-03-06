#ifndef PARQUET_WRITER_H
#define PARQUET_WRITER_H

#include <nan.h>

#include <arrow/io/file.h>
#include <parquet/api/writer.h>

class ParquetWriter : public Nan::ObjectWrap {
  public:
    static void Init(v8::Local<v8::Object> exports);

  private:
    ParquetWriter(const Nan::FunctionCallbackInfo<v8::Value>& info);
    ~ParquetWriter();

    static void NewInstance(const Nan::FunctionCallbackInfo<v8::Value>& info);
    static void New(const Nan::FunctionCallbackInfo<v8::Value>& info);
    static void Write(const Nan::FunctionCallbackInfo<v8::Value>& info);
    static void Close(const Nan::FunctionCallbackInfo<v8::Value>& info);
    static Nan::Persistent<v8::Function> constructor;

    // Wrapped object
    std::shared_ptr<parquet::ParquetFileWriter> pw_;
    std::shared_ptr<arrow::io::FileOutputStream> fw_;
    int ncols_;
};

#endif
