#!/usr/bin/env node

// Copyright 2017 Luca-SAS, licensed under the Apache License 2.0

const parquet = require('../lib/parquet.js');

const file = process.argv[2];
const reader = parquet.Reader(file);
console.log(reader.metadata);
