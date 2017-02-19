#!/usr/bin/env node

const parquet = require('../lib/parquet.js');

const file = process.argv[2];
const reader = parquet.Reader(file);
console.log(reader.metadata);
