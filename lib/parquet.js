// Copyright 2017 Luca-SAS, licensed under the Apache License 2.0

// Pure javascript module to handle parquet format.
// from specifications at https://parquet.apache.org/documentation/latest/

'use strict';

var fs = require('fs');
var util = require('util');
var varint = require('varint');
var hexdump = require('hexdump-nodejs');

module.exports = {
  Reader: Reader
};

// Thrift Compact Protocol Types
var Type = {
  stop:     0,
  true:     1,
  false:    2,
  byte:     3,
  int16:    4,
  int32:    5,
  int64:    6,
  double:   7,
  binary:   8,
  list:     9,
  set:      10,
  map:      11,
  struct:   12,
  extended: 15
};

var TypeString = {
  0:  'stop',
  1:  'true',
  2:  'false',
  3:  'byte',
  4:  'int16',
  5:  'int32',
  6:  'int64',
  7:  'double',
  8:  'binary',
  9:  'list',
  10: 'set',
  11: 'map',
  12: 'struct',
  15: 'extended'
};

function Reader(path) {
  if (!(this instanceof Reader)) return new Reader(path);
  this._path = path;
  this._fd = fs.openSync(path, 'r');
  this.metadata = parseMetadata(this._fd);
}

// zigzag to integer number
function zz2num(n) {
  return (n >>> 1) ^ (-1 * (n & 1));
}

// left side of first byte
function lsb1(n) {
  return (n >>> 4) & 0xf;
}

// Thrift compact protocol parser
var readType = {
  0: function readStop(buf, offset) {
    return {stop: true, len: 0};
  },
  1: function readTrue(buf, offset) {
    return {type: 'bool', data: true, len: 0};
  },
  2: function readFalse(buf, offset) {
    return {type: 'bool', data: false, len: 0};
  },
  3: function readByte(buf, offset) {
    return {type: 'byte', data: buf[offset + 1], len: 1};
  },
  4: function readInt16(buf, offset) {
    return {type: 'int16', data: zz2num(varint.decode(buf, offset)), len: varint.decode.bytes}; 
  },
  5: function readInt32(buf, offset) {
    return {type: 'int32', data: zz2num(varint.decode(buf, offset)), len: varint.decode.bytes}; 
  },
  6: function readInt64(buf, offset) {
    return {type: 'int64', data: zz2num(varint.decode(buf, offset)), len: varint.decode.bytes}; 
  },
  8: function readBinary(buf, offset) {
    var len = varint.decode(buf, offset);
    var start = offset + varint.decode.bytes;
    return {type: 'binary', data: buf.slice(start, start + len).toString(), len: varint.decode.bytes + len};
  },
  9: function readList(buf, offset) {
    var n = lsb1(buf[offset]);
    var len = 1;
    if (n === 15) {
      n = varint.decode(buf, offset + len);
      len += varint.decode.bytes;
    }
    var type = buf[offset] & 0xf;
    var res = {};
    for (var i = 0; i < n; i++) {
      res[i] = readType[type](buf, offset + len);
      len += res[i].len;
    }
    return {type: [TypeString[type]], num: n, data: res, len: len}
  },
  12: function readStruct(buf, offset) {
    return readObj(buf, offset, 'struct');
  }
};

function readObj(buf, offset, rtype) {
    var res = {}, cur, type, len = 0, i = 0;
    do {
      type = buf[offset + len] & 0xf;
      len++;
      cur = readType[type](buf, offset + len);
      if (!cur.stop) res[i++] = cur;
      len += cur.len;
    } while (!cur.stop && (offset + len) < buf.length);
    return {type: rtype, data: res, len: len};
}

var parquetTypes = ['BOOLEAN', 'INT32', 'INT64', 'INT96', 'FLOAT', 'DOUBLE', 'BYTE_ARRAY', 'FIXED_LEN_BYTE_ARRAY'];
var convertedTypes = ['UTF8', 'MAP', 'MAP_KEY_VALUE', 'LIST', 'ENUM', 'DECIMAL', 'DATE', 'TIME_MILLIS', 'TIME_MICROS', 'TIMESTAMP_MILLIS',  'TIMESTAMP_MICROS', 'UINT_8', 'UINT_16', 'UINT_32', 'UINT_64', 'INT_8', 'INT_16', 'INT_32', 'INT_64', 'JSON', 'BSON', 'INTERVAL'];
var parquetEncodings = ['PLAIN', 'GROUP_VAR_INT', 'PLAIN_DICTIONARY', 'RLE', 'BIT_PACKED', 'DELTA_BINARY_PACKED', 'DELTA_LENGTH_BYTE_ARRAY', 'DELTA_BYTE_ARRAY', 'RLE_DICTIONARY'];

// schema of parquet metadata descriptor
var parquetSchema = {
  version: {type: 'int32'},
  schema: {
    type: ['struct'], schema: {
      // Data type for this field. Not set if the current element is a non-leaf node (i.e the 1st)
      type: {type: 'int32', optional: true, values: parquetTypes, check: function (context) {
        if (context.path[0] === 'schema' && context.path[1] === 0 && !context.body.schema[0]) return false;
        return true;
      }},
      // if type is FIXED_LEN_BYTE_ARRAY, this is the byte length of the values,
      // otherwise, if specified, this is the maximum bit length to store any of the values.
      typeLength: {type: 'int32', optional: true, check: function (context) {
        var cur = context.body[context.path[0]][context.path[1]];
        return (cur && cur.type === 'FIXED_LEN_BYTE_ARRAY');
      }},
      fieldRepetitionType: {type: 'int32', optional: true, values: ['REQUIRED', 'OPTIONAL', 'REPEATED']},
      name: {type: 'binary'},
      numChildren: {type: 'int32', optional: true, check: function (context) {
        if (!context.body.schema[0]) return true;
        var cur = context.body[context.path[0]][context.path[1]];
        return (cur && (cur.type === 'INT64' || cur.type === 'INT32'));
      }},
      convertedType: {type: 'int32', optional: true, values: convertedTypes},
      // Used only for DECIMAL converted type
      scale: {type: 'int32', optional: true, check: function (context) {
        var cur = context.body[context.path[0]][context.path[1]];
        return (cur && cur.convertedType === 'DECIMAL');
      }},
      // Used only for DECIMAL converted type
      precision: {type: 'int32', optional: true, check: function (context) {
        var cur = context.body[context.path[0]][context.path[1]];
        return (cur && cur.convertedType === 'DECIMAL');
      }},
      fieldId: {type: 'int32', optional: true},
    }
  },
  numRows: {type: 'int64'},
  rowGroups: {
    type: ['struct'], schema: {
      columns: {
        type: ['struct'], schema: {
          filePath: {type: 'binary', optional: true},
          fileOffset: {type: 'int64'},
          metadata: {
            type: 'struct', optional: true, schema: {
              type: {type: 'int32', values: parquetTypes},
              encodings: {type: ['int32'], values: parquetEncodings},
              schemaPath: {type: ['binary']},
              compressionCodec: {type: 'int32', values: ['UNCOMPRESSED', 'SNAPPY', 'GZIP', 'LZO', 'BROTLI']},
              numValues: {type: 'int64'},
              uncompressedSize: {type: 'int64'},
              compressedSize: {type: 'int64'},
              keyValueMetadata: {type: 'struct', optional: true, schema: {
                  key: {type: 'binary'},
                  value: {type: 'binary', optional: true}
                }
              },
              dataPageOffset: {type: 'int64'},
              indexPageOffset: {type: 'int64', optional: true},
              dictionaryPageOffset: {type: 'int64', optional: true},
              statistics: {type: 'struct', optional: true, schema: {
                  max: {type: 'binary', optional: true},
                  min: {type: 'binary', optional: true},
                  nullCount: {type: 'int64', optional: true},
                  distinctCount: {type: 'int64', optional: true}
                }
              },
              encodingStats: {type: ['struct'], optional: true, schema: {
                  pageType: {type: 'int32', values: ['DATA_PAGE', 'INDEX_PAGE', 'DICTIONARY_PAGE', 'DATA_PAGE_V2']},
                  encoding: {type: 'int32', values: parquetEncodings},
                  count: {type: 'int32'}
                }
              }
            }
          }
        }
      },
      totalByteSize: {type: 'int64'},
      numRows: {type: 'int64'},
      sortingColumns: { type: ['struct'], optional: true}
    },
  },
  createdBy: {type: 'binary', optional: true}
};

var statisticSchema = {
  max: {type: 'binary', optional: true},
  min: {type: 'binary', optional: true},
  nullCount: {type: 'int64', optional: true},
  distinctCount: {type: 'int64', optional: true}
};

var pageHeaderSchema = {
  type: {type: 'int32', values: ['DATA_PAGE', 'INDEX_PAGE', 'DICTIONARY_PAGE', 'DATA_PAGE_V2']},
  uncompressedSize: {type: 'int32'},
  compressedSize: {type: 'int32'},
  crc: {type: 'int32', optional: true},
  // The rest of the header is determined by the above type
  dataPageHeader: {
    type: 'struct', optional: true, schema: {
      numValues: {type: 'int32'},
      encoding: {type: 'int32', values: parquetEncodings},
      definitionLevelEncoding: {type: 'int32', values: parquetEncodings},
      repetitionLevelEncoding: {type: 'int32', values: parquetEncodings},
      statistics: {type: 'struct', optional: true, schema: statisticSchema}
    }, check: function (context) {
      return context.body.type === 'DATA_PAGE';
    }
  },
  indexPageHeader: {
    type: 'struct', optional: true, schema: { // FIXME: to be completed
      numValues: {type: 'int32'},
    }, check: function (context) {
      return context.body.type === 'INDEX_PAGE';
    }
  },
  dictionaryPageHeader: {
    type: 'struct', optional: true, schema: { // FIXME: to be completed
      numValues: {type: 'int32'},
      encoding: {type: 'int32', values: parquetEncodings},
    }, check: function (context) {
      return context.body.type === 'DICTIONARY_PAGE';
    }
  },
  dataPageV2Header: {
    type: 'struct', optional: true, schema: { // FIXME: to be completed
      numValues: {type: 'int32'},
    }, check: function (context) {
      return context.body.type === 'DATA_PAGE_V2';
    }
  },
}

function typeEqual(t1, t2) {
  return Array.isArray(t1) ? (t1[0] === t2[0]) : (t1 === t2);
}

function decode(fullSchema, input, context) {
  var out = {}, i, j, name, obj, schema;
  var schemaIndex = 0;
  var names = Object.keys(fullSchema);
  var ikeys = Object.keys(input.data);
  var pathIndex;

  if (!context) context = {body: out, path: []};

  for (i = 0; i < ikeys.length; i++) {
    obj = input.data[ikeys[i]];
    while (schemaIndex < names.length) {
      name = names[schemaIndex++];
      schema = fullSchema[name];
      if (!typeEqual(obj.type, schema.type)) {
        if (schema.optional) continue;
        else console.log("##### ERROR SCHEMA");
      } else if (schema.check) {
        if (!schema.check(context)) continue;
      }
      break;
    }
    context.path.push(name);

    if (Array.isArray(obj.type)) {
      out[name] = new Array(obj.num);
      pathIndex = context.path.push(0) - 1;
      for (j = 0; j < obj.num; j++) {
        if (obj.type[0] === 'struct') {
          out[name][j] = decode(schema.schema, obj.data[j], context);
        } else {
          if (schema.values)
            out[name][j] = schema.values[obj.data[j].data];
          else
            out[name][j] = obj.data[j].data;
        }
        context.path[pathIndex] = j;
      }
      context.path.pop();
    } else if (obj.type === 'struct') {
      out[name] = decode(schema.schema, obj, context);
    } else {
      if (schema.values)
        out[name] = schema.values[obj.data];
      else
        out[name] = obj.data;
    }
   context.path.pop();
  }
  return out;
}

function parseMetadata(fd) {
  var stat = fs.fstatSync(fd);
  //console.log('stat', stat);
  var footer = Buffer.alloc(8);
  var n = fs.readSync(fd, footer, 0, 8, stat.size - 8); 
  var mlen;
  var mbuf;

  //console.log('n:', n);
  //console.log('footer:', footer);
  mlen = footer.readInt32LE(0);
  //console.log('footer len:', mlen);
  mbuf = Buffer.alloc(mlen);

  n = fs.readSync(fd, mbuf, 0, mlen, stat.size - mlen - 8);
  //console.log('n:', n);
  //console.log(hexdump(mbuf));
  //var offset = 0;
  //var tmp;

  //var res = readObj(mbuf, 0);
  //console.log('res', JSON.stringify(res, null, 2));

  //var meta = decode(parquetSchema, res);
  //console.log('meta', meta);
  //console.log('meta', JSON.stringify(meta, null, 2));
  return decode(parquetSchema, readObj(mbuf, 0));
}

Reader.prototype.readCol = function (col, skip, rows) {
  var meta = this.metadata.rowGroups[0].columns[col];
  console.log('col metadata:', meta);
  var stat = fs.statSync(this._path);
  var fileSize = stat.size;
  var debugBuf = Buffer.alloc(fileSize);
  var n = fs.readSync(this._fd, debugBuf, 0, debugBuf.length, 0);
  console.log(hexdump(debugBuf));
  //var pageDataBuf = 
  var len = meta.metadata.uncompressedSize;
  var start = meta.fileOffset - len;
  var buf = Buffer.alloc(len); 
  n = fs.readSync(this._fd, buf, 0, len, start);
  console.log(hexdump(buf));
  console.log('obj:', readObj(buf, 0));
  var indexPageHeader = decode(pageHeaderSchema, readObj(buf, 0));
  console.log('indexPageHeader:', indexPageHeader);
  // todo: decode page values: if dictionary page preceding data page,
  // start at datapage offset - value length (in the page).
  // otherwise: buffer end - value length
  var vstart = meta.metadata.dataPageOffset - start - indexPageHeader.uncompressedSize;
  var values = [];
  switch (meta.metadata.type) {
  case 'INT32':
    for (var i = 0; i < meta.metadata.numValues; i++) {
      values[i] = buf.readInt32LE(vstart + i * 4);
    }
    break;
  }
  console.log('dict values:', values);
  var dataPageHeader = decode(pageHeaderSchema, readObj(buf, meta.metadata.dataPageOffset - start));
  console.log('dataPageHeader:', dataPageHeader);
  var bitWidth = Math.ceil(Math.log2(dataPageHeader.numValues));
  var dlen = dataPageHeader.uncompressedSize - 1;
  rleDecode(buf, len - dlen, dlen, bitWidth);
}

// Hybrid RLE / Bit Packed decoder
function rleDecode(buf, offset, datalen, bitWidth) {
  var header = varint.decode(buf, offset);
  var count = header >>> 1;

  datalen -= varint.decode.bytes;
  if (header & 1) { // Bit packed
    console.log('read ' + (count * 8) + ' bit packed values');
  } else {          // RLE
    console.log('read ' + count + ' RLE values');
    console.log('remain ' + datalen + ' bytes');
  }
}
