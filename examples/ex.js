#!/usr/bin/env node

var parquet = require('../build/Release/parquet');
console.log('parquet', parquet);

var f = parquet.createReader(__dirname + '/parquet_cpp_example.parquet');
console.log('f:', f);
console.log('info:', f.info());
//console.log('debugPrint:', f.debugPrint());
var r = f.readSync(0, 0, 4);
var l = f.readline(2, 3);
console.log('l:', l);
//l =  f.readline();
//console.log('l:', l);
