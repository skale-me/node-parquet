#!/usr/bin/env node

var t = require('tap');
var parquet = require('../build/Release/parquet');

var schema = {ba: {type: 'byte_array'}};
var writer = parquet.createWriter(__dirname + '/test.parquet', schema);

t.type(writer, 'object');
writer.writeSync([[Buffer.from('hello')], ['world']]);
writer.close();
