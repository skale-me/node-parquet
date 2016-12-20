#!/usr/bin/env node

var parquet = require('./build/Release/parquet');
console.log('parquet', parquet);

var f = parquet.createReader('parquet_cpp_example.parquet');
console.log('f:', f);
console.log('info:', f.info());
//console.log('debugPrint:', f.debugPrint());
