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

var reader = new parquet.ParquetReader('my_file.parquet');
console.log(reader.info());
console.log(reader.rows();
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

## API reference

To use this module, one must `require('node-parquet')`

### Class: parquet.ParquetReader

`ParquetReader` object performs read operations on a file in parquet format.

#### new parquet.ParquetReader(filename)

Construct a new parquet reader object.

* `filename`: `String` containing the parquet file path

Example:

```javascript
const parquet = require('node-parquet');
const reader = new parquet.ParquetReader('./parquet_cpp_example.parquet');
```

#### reader.close()

Close the reader object.

#### reader.info()

Return an `Object` containing parquet file metadata. The object looks like:

```javascript
{
  version: 0,
  createdBy: 'Apache parquet-cpp',
  rowGroups: 1,
  columns: 8,
  rows: 500
}
```

#### reader.rows([nb_rows])

Return an `Array` of rows, where each row is itself an `Array` of column elements.

* `nb_rows`: `Number` defining the maximum number of rows to return.

### Class: parquet.ParquetWriter

`ParquetWriter` object implements write operation on a parquet file.

#### new parquet.ParquetWriter(filename, schema, [compression])

Construct a new parquet writer object.

* `filename`: `String` containing the parquet file path
* `schema`: `Object` defining the data structure, where keys are column names and values are `Objects` with the following fields:
  * `"type"`: required `String` indicating the type of column data, can be any of:
      - `"bool"`: boolean value, converted from `Boolean`
      - `"int32"`: 32 bits integer value, converted from `Number`
      - `"int64"`: 64 bits integer value, converted from `Number`
      - `"float"`: 32 bits floating number value, converted from `Number`
      - `"double"`: 64 bits floating number value, converted from `Number`
      - `"byte_array"`: array of bytes, converted from a `String`
      - `"group"`: array of nested structures, described with a `"schema"` field
  * `"optional"`: `Boolean` indicating if the field can be omitted in a record. Default: `false`.
  * `"repeated"`: `Boolean` indicating if the field can be repeated in a record, thus forming an array. Ignored if not defined within a schema of type `"group"` (schema itself or one of its parent).
  * `"schema"`: `Object` which content is a `schema` defining the nested structure. Required for objects where type is `"group"`, ignored for others.
* `compression`: optional `String` indicating the compression algorithm to apply to columns. Can be one of `"snappy"`, `"gzip"`, `"brotli"` or `"lzo"`. By default compression is disabled.

For example, considering the following object: `{ name: "foo", content: [ 1, 2, 3] }`, its descriptor schema is:

```javascript
const schema = {
  name: { type: "byte_array" },
  content: {
    type: "group",
    repeated: "true",
    schema: { i0: { type: "int32" } }
  }
};
```

#### writer.close()

Close a file opened for writing. Calling this method explicitely before exiting is mandatory to ensure that memory content is correctly written in the file.

#### writer.writeSync(rows)

Write the content of `rows` in the file opened by the writer. Data from rows will be dispatched into the separate parquet columns according to the schema specified in the contructor.

* `rows`: `Array` of rows, where each row is itself an `Array` of column elements, according to the schema.

For example, considering the above nested schema, a write operation could be:

```javascript
writer.write([
  [ "foo", [ 1, 2, 3] ],
  [ "bar", [ 100, 400, 600, 2 ] ]
]);
```

## Caveats and limitations

- no schema extract at reading yet
- int64 bigger than 2^53 - 1 are not accaturately represented (big number library like [bignum](https://www.npmjs.com/package/bignum) integration planned)
- purejs implementation not complete, although most of metadata is now correctly parsed.
- read and write are only synchronous
- Compression at write is not supported yet (although possible through parquet-cpp)
- the native library parquet-cpp does not build on MS-Windows
- many tests are missing
- benchmarks are missing
- neat commmand line tool missing

Plan is to improve this over time. Contributions are welcome.

## License

[Apache-2.0](LICENSE)
