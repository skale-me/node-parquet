# Node-parquet

[![Build Status](https://travis-ci.org/mvertes/node-parquet.svg?branch=master)](https://travis-ci.org/mvertes/node-parquet)

[Parquet](http://parquet.apache.org) is a [columnar
storage](https://en.wikipedia.org/wiki/Column-oriented_DBMS) format
available to any project in the Hadoop ecosystem. This nodejs module
provides native bindings to the parquet functions from
[parquet-cpp](https://github.com/apache/parquet-cpp).

A pure javascript parquet format driver (still in development) is also provided.

## Build, install

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

```shell
npm install
```

From 0.2.4 version, a command line tool called `parquet` is provided.
It can be installed globally by running `npm install -g`. Note that
if you install node-parquet this way, you can still use it as a dependency
module in your local projects by linking (`npm link node-parquet`) which
avoids the cost of recompiling the complete parquet-cpp library and
its dependencies.

Otherwise, for developers to build directly from a github clone:

```shell
git clone https://github.com/mvertes/node-parquet.git
cd node-parquet
git submodule update --init --recursive
npm install [-g]
```

After install, the parquet-cpp build directory `build_deps` can be
removed by running `npm run clean`, recovering all disk space taken
for building parquet-cpp and its dependencies.

## Usage

### Command line tool

A command line tool `parquet` is provided. It's quite minimalist
right now and needs to be improved:

```
Usage: parquet [options] <command> [<args>]

Command line tool to manipulate parquet files

Commands:
  cat file       Print file content on standard output
  head file      Print the first lines of file
  info file      Print file metadata

Options:
  -h,--help      Print this help text
  -V,--version   Print version and exits
```

### Reading

The following example shows how to read a `parquet` file:

```javascript
var parquet = require('node-parquet');

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

var writer = new parquet.ParquetWriter('my_file.parquet', schema);
writer.write(data);
writer.close();
```

## API reference

The API is not yet considered stable nor complete.

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

#### reader.read(column_number)

This is a low level function, it should not be used directly.

Read and return the next element in the column indicated by `column_number`.

In the case of a non-nested column, a basic value (`Boolean`, `Number`, `String` or `Buffer`) is returned, otherwise, an array of 3 elemnents is returned, where a[0] is the parquet definition level, a[1] the parquet repetition level, and a[2] the basic value. Definition and repetition levels are useful to reconstruct rows of composite, possibly sparse records with nested columns.

* `column_number`: the column number in the row

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
      - `"timestamp"`: 64 bits integer value, converted from `Date`, with parquet logical type `TIMESTAMP_MILLIS`, the number of milliseconds from the Unix epoch, 00:00:00.000 on 1 January 1970, UTC
      - `"float"`: 32 bits floating number value, converted from `Number`
      - `"double"`: 64 bits floating number value, converted from `Number`
      - `"byte_array"`: array of bytes, converted from a `String` or buffer
      - `"string"`: array of bytes, converted from a `String`, with parquet logical type `UTF8`
      - `"group"`: array of nested structures, described with a `"schema"` field
  * `"optional"`: `Boolean` indicating if the field can be omitted in a record. Default: `false`.
  * `"repeated"`: `Boolean` indicating if the field can be repeated in a record, thus forming an array. Ignored if not defined within a schema of type `"group"` (schema itself or one of its parent).
  * `"schema"`: `Object` which content is a `schema` defining the nested structure. Required for objects where type is `"group"`, ignored for others.
* `compression`: optional `String` indicating the compression algorithm to apply to columns. Can be one of `"snappy"`, `"gzip"`, `"brotli"` or `"lzo"`. By default compression is disabled.

For example, considering the following object: `{ name: "foo", content: [ 1, 2, 3] }`, its descriptor schema is:

```javascript
const schema = {
  name: { type: "string" },
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
- int64 bigger than 2^53 - 1 are not represented accurately (big number library like [bignum](https://www.npmjs.com/package/bignum) integration planned)
- purejs implementation not complete, although most of metadata is now correctly parsed.
- read and write are only synchronous
- the native library parquet-cpp does not build on MS-Windows
- many tests are missing
- benchmarks are missing
- neat commmand line tool missing (one provided since 0.2.4)

Plan is to improve this over time. Contributions are welcome.

## License

[Apache-2.0](LICENSE)
