#!/usr/bin/env node

var t = require('tap');
var parquet = require('../');

var schema = {int: {type: 'int32'}};
var writer = new parquet.ParquetWriter(__dirname + '/test.parquet', schema);

t.type(writer, 'object');
t.equal(writer.write([[1], [2], [3]]), 3);
writer.close();
