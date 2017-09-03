# Change Log

## [0.2.5](https://github.com/skale-me/node-parquet/tree/0.2.5) (2017-09-03)
[Full Changelog](https://github.com/skale-me/node-parquet/compare/0.2.4...0.2.5)

**Fixed bugs:**

- ParquetWriter: memory fault when attempt to write an object instead of a string [\#21](https://github.com/skale-me/node-parquet/issues/21)

**Closed issues:**

- fatal error: 'cstdint' file not found [\#28](https://github.com/skale-me/node-parquet/issues/28)

**Merged pull requests:**

- improve README [\#42](https://github.com/skale-me/node-parquet/pull/42) ([mvertes](https://github.com/mvertes))
- improve tests, check read data against original used for write [\#41](https://github.com/skale-me/node-parquet/pull/41) ([mvertes](https://github.com/mvertes))
- travis CI: build and test on linux and MacOSX [\#40](https://github.com/skale-me/node-parquet/pull/40) ([mvertes](https://github.com/mvertes))
- remove obsolete non-working code [\#39](https://github.com/skale-me/node-parquet/pull/39) ([mvertes](https://github.com/mvertes))
- writer: fix compile warning [\#38](https://github.com/skale-me/node-parquet/pull/38) ([mvertes](https://github.com/mvertes))
- reader: fix uninitialized variables. Should correct \#32. [\#37](https://github.com/skale-me/node-parquet/pull/37) ([mvertes](https://github.com/mvertes))
- Add 'string' and 'timestamp' logical types [\#36](https://github.com/skale-me/node-parquet/pull/36) ([mvertes](https://github.com/mvertes))
- fix build of static lib dependencies [\#34](https://github.com/skale-me/node-parquet/pull/34) ([mvertes](https://github.com/mvertes))
- update to parquet-cpp-1.2.0 [\#33](https://github.com/skale-me/node-parquet/pull/33) ([mvertes](https://github.com/mvertes))
- post-install: clean up compiled dependency libs in production mode [\#31](https://github.com/skale-me/node-parquet/pull/31) ([mvertes](https://github.com/mvertes))
- fixes typo [\#29](https://github.com/skale-me/node-parquet/pull/29) ([danielsan](https://github.com/danielsan))
- update parquet-cpp dependency [\#27](https://github.com/skale-me/node-parquet/pull/27) ([mvertes](https://github.com/mvertes))
- Fix headings so they're recognised as valid markdown [\#26](https://github.com/skale-me/node-parquet/pull/26) ([spinningarrow](https://github.com/spinningarrow))

## [0.2.4](https://github.com/skale-me/node-parquet/tree/0.2.4) (2017-04-04)
[Full Changelog](https://github.com/skale-me/node-parquet/compare/0.2.3...0.2.4)

**Merged pull requests:**

- Update parquet-cpp dependency [\#25](https://github.com/skale-me/node-parquet/pull/25) ([mvertes](https://github.com/mvertes))
- Add a command line tool parquet [\#24](https://github.com/skale-me/node-parquet/pull/24) ([mvertes](https://github.com/mvertes))
- Link module against static libraries rather than dynamic ones. [\#23](https://github.com/skale-me/node-parquet/pull/23) ([mvertes](https://github.com/mvertes))
- Fix issue \#21 where writing an invalid string caused a segmentation fault [\#22](https://github.com/skale-me/node-parquet/pull/22) ([mvertes](https://github.com/mvertes))

## [0.2.3](https://github.com/skale-me/node-parquet/tree/0.2.3) (2017-03-10)
[Full Changelog](https://github.com/skale-me/node-parquet/compare/0.2.2...0.2.3)

**Merged pull requests:**

- write strings: fix memory problems. [\#19](https://github.com/skale-me/node-parquet/pull/19) ([mvertes](https://github.com/mvertes))

## [0.2.2](https://github.com/skale-me/node-parquet/tree/0.2.2) (2017-03-08)
[Full Changelog](https://github.com/skale-me/node-parquet/compare/0.2.1...0.2.2)

**Merged pull requests:**

- Fix a possible memory corruption at write [\#18](https://github.com/skale-me/node-parquet/pull/18) ([mvertes](https://github.com/mvertes))

## [0.2.1](https://github.com/skale-me/node-parquet/tree/0.2.1) (2017-03-08)
[Full Changelog](https://github.com/skale-me/node-parquet/compare/0.2.0...0.2.1)

**Merged pull requests:**

- fix dependencies [\#17](https://github.com/skale-me/node-parquet/pull/17) ([mvertes](https://github.com/mvertes))
- README: fix examples, document read\(\) [\#16](https://github.com/skale-me/node-parquet/pull/16) ([mvertes](https://github.com/mvertes))

## [0.2.0](https://github.com/skale-me/node-parquet/tree/0.2.0) (2017-03-07)
[Full Changelog](https://github.com/skale-me/node-parquet/compare/0.1.1...0.2.0)

**Merged pull requests:**

- write enable optional compression: gzip, snappy, brotli or lzo [\#15](https://github.com/skale-me/node-parquet/pull/15) ([mvertes](https://github.com/mvertes))
- writer: fix string encoding [\#14](https://github.com/skale-me/node-parquet/pull/14) ([mvertes](https://github.com/mvertes))
- Document API [\#13](https://github.com/skale-me/node-parquet/pull/13) ([mvertes](https://github.com/mvertes))
- rename writer.writeSync to writer.write before next release [\#12](https://github.com/skale-me/node-parquet/pull/12) ([mvertes](https://github.com/mvertes))
- reader.read\(\): Fix handling of remaining rows [\#11](https://github.com/skale-me/node-parquet/pull/11) ([mvertes](https://github.com/mvertes))
- Fix nested columns at read and write. Code cleaning [\#10](https://github.com/skale-me/node-parquet/pull/10) ([mvertes](https://github.com/mvertes))
- update parquet-cpp [\#9](https://github.com/skale-me/node-parquet/pull/9) ([mvertes](https://github.com/mvertes))
- Fix reading [\#8](https://github.com/skale-me/node-parquet/pull/8) ([mvertes](https://github.com/mvertes))
- fix performance at read [\#7](https://github.com/skale-me/node-parquet/pull/7) ([mvertes](https://github.com/mvertes))
- writer: fix a bug where values were ignored after null [\#5](https://github.com/skale-me/node-parquet/pull/5) ([mvertes](https://github.com/mvertes))
- js reader: fix file metadata schema parse [\#4](https://github.com/skale-me/node-parquet/pull/4) ([mvertes](https://github.com/mvertes))
- reader: return arrays instead of objects, handle UTF8 strings [\#3](https://github.com/skale-me/node-parquet/pull/3) ([mvertes](https://github.com/mvertes))
- update parquet-cpp dependency [\#2](https://github.com/skale-me/node-parquet/pull/2) ([mvertes](https://github.com/mvertes))

## [0.1.1](https://github.com/skale-me/node-parquet/tree/0.1.1) (2017-02-10)
[Full Changelog](https://github.com/skale-me/node-parquet/compare/0.1.0...0.1.1)

## [0.1.0](https://github.com/skale-me/node-parquet/tree/0.1.0) (2017-02-10)
**Merged pull requests:**

- js reader: handle thrift list with more than 14 fields [\#1](https://github.com/skale-me/node-parquet/pull/1) ([mvertes](https://github.com/mvertes))



\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*