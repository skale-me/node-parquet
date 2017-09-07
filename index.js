// Copyright 2017 Luca-SAS, licensed under the Apache License 2.0

'use strict';

const parquet = require('./build/Release/parquet.node');

module.exports = parquet;

parquet.ParquetReader.prototype.rows = function(nrows) {
  const info = this.info();
  nrows = nrows || info.rows;
  var i, j, col, e;

  if (!this._last) this._last = [];
  if (!this._count) this._count = [];
  if (this._remain === undefined) this._remain = info.rows;
  if (nrows > this._remain) nrows = this._remain;

  const rows = new Array(nrows);
  for (j = 0; j < info.columns; j++) {
    this._count[j] = 0;
  }
  for (i = 0; i < nrows; i++) {
    rows[i] = new Array(info.columns);
    col = rows[i];
    for (j = 0; j < info.columns; j++) {
      if (this._last[j]) {
        col[j] = [this._last[j][2]];
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
  this._remain -= nrows;
  return rows;
};
