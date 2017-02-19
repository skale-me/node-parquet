'use strict';

const parquet = require('./build/Release/parquet.node');

module.exports = parquet;

parquet.ParquetReader.prototype.readRows = function () {
  const info = this.info();
  const blocsize = 1 << 16;
  const nblocs = info.rows >>> 16;
  const lastsize = info.rows % blocsize;
  const rows = new Array(info.rows);
  var col, base, end, i, j, k;

  for (i = 0; i < nblocs; i++) {
    base = i * blocsize;
    end = base + blocsize;
    for (k = base; k < end; k++)
      rows[k] = new Array(info.columns);
    for (j = 0; j < info.columns; j++) {
      col = this.readColumn(j, 0, blocsize);
      for (k = base; k < end; k++) {
        rows[k][j] = col[k];
      }
    }
  }
  if (!lastsize) return rows;
  base = nblocs * blocsize;
  end = base + lastsize;
  for (k = base; k < end; k++)
      rows[k] = new Array(info.columns);
    rows[k] = [];
  for (j = 0; j < info.columns; j++) {
    col = this.readColumn(j, 0, lastsize);
    for (k = base; k < end; k++) {
      rows[k][j] = col[k];
    }
  }
  return rows;
};
