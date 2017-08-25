#!/usr/bin/env node

var t = require('tap');
var parquet = require('../');

var schema = {int: {type: 'timestamp'}};
var writer = new parquet.ParquetWriter(__dirname + '/test.parquet', schema);

t.type(writer, 'object');
t.equal(writer.write([[(new Date).getTime()]]), 1);
writer.close();
