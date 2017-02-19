#!/usr/bin/env node

const parquet = require('../index.js');

//const file = process.argv[2];
const file = __dirname + '/../../og1.parquet';
const out = process.argv[3];

console.time('open reader');
const reader = new parquet.ParquetReader(file);
const info = reader.info();
console.timeEnd('open reader');
console.log('info:', info);

console.time('read');
const dataset = reader.readRows();
console.timeEnd('read');

reader.close();
//console.log('dataset[0]:', dataset[0]);
console.log('dataset length:', dataset.length);

console.log(dataset);
if (!out) process.exit();

const schema = {
  odid: {type: 'byte_array', optional: true},
  id_in_db: {type: 'int64', optional: true},
  data_visit: {type: 'int64', optional: true},
  url: {type: 'byte_array', optional: true},
  first_query_received: {type: 'int64', optional: true},
  last_query_received: {type: 'int64', optional: true},
  nb_of_query_received: {type: 'int64', optional: true},
  array_element: {type: 'byte_array', optional: true}
};


console.time('open writer');
const writer = new parquet.ParquetWriter(out, schema);
console.timeEnd('open writer');

console.time('write');
writer.writeSync(dataset);
writer.close();
console.timeEnd('write');
