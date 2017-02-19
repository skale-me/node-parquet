#!/usr/bin/env node

const parquet = require('../index.js');

const file = process.argv[2];
const reader = new parquet.ParquetReader(file);
const info = reader.info();
console.log(info);
const nbits = 4
const blocsize = 1 << nbits;
const nblocs = info.rows >> nbits;
const lastsize = info.rows % blocsize;
var i, bloc;

//console.log(reader.readline(0, 2));
for (i = 0; i < nblocs; i++) {
  console.log('read ' + blocsize + ' from ' + i * blocsize);
  bloc = reader.readRow(i * blocsize, blocsize);
  console.log(bloc);
}
if (lastsize) {
  bloc = reader.readRow(i * blocsize, lastsize);
  console.log(bloc);
}
