#!/usr/bin/env node

var t = require('tap');
var parquet = require('../');

var file = __dirname + '/test.parquet';
var schema = {int: {type: 'timestamp'}};
var writer = new parquet.ParquetWriter(file, schema);
var now = new Date;

t.type(writer, 'object');
t.equal(writer.write([[now.getTime()]]), 1);
writer.close();

var reader = new parquet.ParquetReader(file);
var info = reader.info();
t.type(reader, 'object');
t.equal(info.rowGroups, 1);
t.equal(info.columns, 1);
t.equal(info.rows, 1);
var data = reader.rows(info.rows);
t.equal(data[0][0], now.getTime());
