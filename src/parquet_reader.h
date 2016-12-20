#ifndef PARQUET_READER_H
#define PARQUET_READER_H

#include <node.h>
#include <node_object_wrap.h>

#include <parquet/api/reader.h>

class ParquetReader : public node::ObjectWrap {
  public:
    static void Init(v8::Isolate* isolate);
    static void NewInstance(const v8::FunctionCallbackInfo<v8::Value>& args);
    std::shared_ptr<parquet::ParquetFileReader> GetWrapped() const { return pr_; };

  private:
    ParquetReader(const v8::FunctionCallbackInfo<v8::Value>& args);
    ~ParquetReader();

    static void New(const v8::FunctionCallbackInfo<v8::Value>& args);
    static void Info(const v8::FunctionCallbackInfo<v8::Value>& args);
    static v8::Persistent<v8::Function> constructor;

    // Wrapped object
    std::shared_ptr<parquet::ParquetFileReader> pr_;
};

#endif
