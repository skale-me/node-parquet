#!/usr/bin/env node

// Modified from original script by @rafiton

var t = require('tap');
var parquet = require('../index.js');

var schema = {
    small_int: {type: 'int32'},
    big_int: {type: 'int64'},
    name: {type: 'string'}
};

var data = [
    [ 13, 1111, 'hello world r'],
    [ 2, 2234, 'hello world 1'],
    [ 3, 2334, 'hello world 2'],
    [ 4, 1223, 'hello world 3']
];

var file = __dirname + '/test.parquet';
var writer = new parquet.ParquetWriter(file, schema);
var nbwritten = writer.write(data);
t.equal(nbwritten, data.length, 'write: correct number of written rows');
writer.close();

var reader = new parquet.ParquetReader(file);
var info = reader.info();
t.equal(info.rows, data.length, 'read: correct number of rows in schema');
t.equal(info.columns, data[0].length, 'read: correct number of columns in schema');

var dataread = reader.rows(info.rows);
t.equal(JSON.stringify(dataread), JSON.stringify(data), 'read: data read identical to original data');
