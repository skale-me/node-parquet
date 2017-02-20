'use strict';

const parquet = require('./build/Release/parquet.node');

module.exports = parquet;

parquet.ParquetReader.prototype.readRows = function () {
  const info = this.info();
  const pow2 = 16;
  const blocsize = 1 << pow2;
  const nblocs = info.rows >>> pow2;
  const lastsize = info.rows % blocsize;
  const rows = new Array(info.rows);
  var col, base, i, j, k;

  for (i = 0; i < nblocs; i++) {
    base = i * blocsize;
    for (k = 0; k < blocsize; k++)
      rows[base + k] = new Array(info.columns);
    for (j = 0; j < info.columns; j++) {
      col = this.readColumn(j, 0, blocsize);
      for (k = 0; k < blocsize; k++) {
        rows[base + k][j] = col[k];
      }
    }
  }
  if (!lastsize) return rows;
  base = nblocs * blocsize;
  for (k = 0; k < lastsize; k++)
    rows[base + k] = new Array(info.columns);
  for (j = 0; j < info.columns; j++) {
    col = this.readColumn(j, 0, lastsize);
    for (k = 0; k < lastsize; k++) {
      rows[base + k][j] = col[k];
    }
  }
  return rows;
};
