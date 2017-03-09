#!/usr/bin/env node

var t = require('tap');
var parquet = require('..');

var schema = {ba: {type: 'byte_array'}};
var writer = new parquet.ParquetWriter(__dirname + '/test.parquet', schema, 'gzip');

t.type(writer, 'object');
writer.write([
  [Buffer.from('hello')],
  ['world'],
  ['00001a8a-e405-4337-a3ec-07dc7431a9c5'],
  [Buffer.from('00001a8a-e405-4337-a3ec-07dc7431a9c5')],
]);
writer.close();
