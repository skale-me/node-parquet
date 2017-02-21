# Node-parquet

[![Build Status](https://travis-ci.org/mvertes/node-parquet.svg?branch=master)](https://travis-ci.org/mvertes/node-parquet)

[Parquet](http://parquet.apache.org) is a [columnar
storage](https://en.wikipedia.org/wiki/Column-oriented_DBMS) format
available to any project in the Hadoop ecosystem. This nodejs module
provides native bindings to the parquet functions from
[parquet-cpp](https://github.com/apache/parquet-cpp).

A pure javascript parquet format driver (still in development) is also provided.

## Build, install

The native c++ module has the following dependencies which must
be installed before attempting to build:

- g++ and gcc version >= 4.8
- cmake > 2.8.6
- boost
- on MacOSX: thrift version >= 0.7

Note that you need also python2 and c++/make toolchain for building
NodeJS native addons.

The standard way of building and installing, provided that above
depencies are met, is simply to run:

```
npm install
```

Otherwise, for developers to build directly from a github clone:

```shell
git clone https://github.com/mvertes/node-parquet.git
cd node-parquet
git submodule update --init --recursive
npm install
```

## Usage

### Reading

The following example shows how to read a `parquet` file:

```javascript
var parquet = require('node-parqet');

var reader = parquet.reader('my_file.parquet');
console.log(reader.info());
console.log(reader.readRows();
reader.close();
```

### Writing

The following example shows how to write a `parquet` file:

```javascript
var parquet = require('node-parquet');

var schema = {
  small_int: {type: 'int32', optional: true},
  big_int: {type: 'int64'},
  my_boolean: {type: 'bool'},
  name: {type: 'byte_array', optional: true},
};

var data = [
  [ 1, 23234, true, 'hello world'],
  [  , 1234, false, ],
];

var writer = parquet.createWrite('my_file.parquet', schema);
writer.writeSync(data);
writer.close();
```

## Caveats and limitations

- Nested schemas not supported yet
- no schema extract at reading yet
- schemas for write are not yet documented
- int64 bigger than 2^53 - 1 are not accaturately represented (bigint integration planned)
- purejs implementation not complete
- the native library parquet-cpp does not build on MS-windows

## License

[Apache-2.0](LICENSE)
