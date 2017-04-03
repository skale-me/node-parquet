#!/usr/bin/env node

var t = require('tap');

var parquet = require('../');

var schema = { string: {type: 'byte_array'}, };

var f = new parquet.ParquetWriter(__dirname + '/t1.parquet', schema);
t.throws(function () {
  f.write([
    [ "hello" ],    // Ok
    [ [ 4 ] ]       // Fault
  ]);
}, {}, {});
f.close();
