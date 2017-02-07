#!/usr/bin/env node

var parquet = require('../lib/parquet.js');

//var reader = parquet.Reader('parquet_cpp_example.parquet');
//var reader = parquet.Reader('alltypes_plain.parquet');
var reader = parquet.Reader(process.argv[2]);
console.log('reader metatada', reader.metadata);
reader.readCol(0);
