#!/usr/bin/env node

var parquet = require('../build/Release/parquet');
console.log('parquet', parquet);

var schema = {
  // int: {type: 'int32', optional: true}
  int: {type: 'int32', optional: true},
  large: {type: 'int64', optional: true},
  bool: {type: 'bool', optional: true},
  string: {type: 'byte_array'},
};

var f = parquet.createWriter(__dirname + '/t1.parquet', schema);
console.log('f:', f);
console.log('writeSync', f.writeSync([
  [3, , true, "hello" ],
  [1, 11, undefined, "world" ]
]));
f.close();
