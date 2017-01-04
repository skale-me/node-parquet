#ifndef PARQUET_READER_H
#define PARQUET_READER_H

#include <nan.h>
//#include <node.h>
//#include <node_object_wrap.h>

#include <parquet/api/reader.h>

class ParquetReader : public Nan::ObjectWrap {
  public:
    //static void Init(v8::Isolate* isolate);
    static void Init(v8::Local<v8::Object> exports);
    static void NewInstance(const Nan::FunctionCallbackInfo<v8::Value>& args);

  private:
    ParquetReader(const Nan::FunctionCallbackInfo<v8::Value>& args);
    ~ParquetReader();

    static void New(const Nan::FunctionCallbackInfo<v8::Value>& args);
    static void DebugPrint(const Nan::FunctionCallbackInfo<v8::Value>& args);
    static void Info(const Nan::FunctionCallbackInfo<v8::Value>& args);
    static void ReadSync(const Nan::FunctionCallbackInfo<v8::Value>& args);
    static Nan::Persistent<v8::Function> constructor;

    // Wrapped object
    std::unique_ptr<parquet::ParquetFileReader> pr_;
};

#endif
