'use strict';

const parquet = require('./build/Release/parquet.node');

module.exports = parquet;

parquet.ParquetReader.prototype.rows = function(nrows) {
  const info = this.info();
  nrows = nrows || info.rows;
  const rows = new Array(nrows);
  var i, j, col, e;

  if (!this._last) this._last = [];
  if (!this._count) this._count = [];

  for (j = 0; j < info.columns; j++) {
    this._count[j] = 0;
  }
  for (i = 0; i < nrows; i++) {
    rows[i] = new Array(info.columns);
    col = rows[i];
    for (j = 0; j < info.columns; j++) {
      if (this._last[j]) {
        col[j] = [this._last[j][2]];
        //console.log('#1 row: ', i, 'col[', j, ']=', col[j], 'last:', this._last[j]);
        while (true) {
          this._last[j] = this.read(j);
          this._count[j]++;
          if (!this._last[j] || this._last[j][1] === 0)
            break;
          col[j].push(this._last[j][2]);
        }
        continue;
      }
      col[j] = this.read(j);
      //console.log('#2 row: ', i, 'col[', j, ']=', col[j]);
      if (Array.isArray(col[j])) {
        this._last[j] = col[j];
        this._count[j]++;
        col[j] = [this._last[j][2]];
        while (true) {
          this._last[j] = this.read(j);
          this._count[j]++;
          if (!this._last[j] || this._last[j][1] === 0)
            break;
          col[j].push(this._last[j][2]);
        }
      }
    }
  }
  console.log(this._count);
  return rows;
};
