{
  'targets': [
    {
	  'target_name': 'parquet',
	  'sources': [
	  	'src/parquet_binding.cc',
		'src/parquet_reader.cc',
		'src/parquet_writer.cc',
	  ],
	  'include_dirs': [
      "deps/parquet-cpp/src",
	  "build_deps/parquet-cpp/arrow_ep/src/arrow_ep-install/include",
      "<!(node -e \"require('nan')\")"
    ],
	  'cflags!': [ '-fno-exceptions' ],
	  'cflags_cc!': [ '-fno-exceptions' ],
	  'conditions': [
	  	['OS=="mac"', {
		  'xcode_settings': {
		   'GCC_ENABLE_CPP_EXCEPTIONS': 'YES'
		  },
		  'libraries': [
			'../build_deps/parquet-cpp/release/libparquet.a',
			'/usr/local/lib/libthrift.a',
			'-L../build_deps/parquet-cpp/snappy_ep/src/snappy_ep-install/lib',
			'-Wl,-rpath,build_deps/parquet-cpp/snappy_ep/src/snappy_ep-install/lib',
			'-L../build_deps/parquet-cpp/brotli_ep/src/brotli_ep-install/lib',
			'-Wl,-rpath,build_deps/parquet-cpp/arrow_ep/src/arrow_ep-install/lib',
			'-L../build_deps/parquet-cpp/arrow_ep/src/arrow_ep-install/lib',
			'-lsnappy',
			'-lbrotlienc',
			'-lbrotlidec',
			'-lbrotlicommon',
			'-larrow',
			'-larrow_io',
		  ],
		},
		'OS=="linux"', {
		  'libraries': [
			'../build_deps/parquet-cpp/release/libparquet.a',
			'../build_deps/parquet-cpp/thrift_ep/src/thrift_ep-install/lib/libthrift.a',
			'-L../build_deps/parquet-cpp/snappy_ep/src/snappy_ep-install/lib',
			'-Wl,-rpath,build_deps/parquet-cpp/snappy_ep/src/snappy_ep-install/lib',
			'-L../build_deps/parquet-cpp/brotli_ep/src/brotli_ep-install/lib',
			'-Wl,-rpath,build_deps/parquet-cpp/arrow_ep/src/arrow_ep-install/lib',
			'-L../build_deps/parquet-cpp/arrow_ep/src/arrow_ep-install/lib',
			'-lsnappy',
			'-lbrotlienc',
			'-lbrotlidec',
			'-lbrotlicommon',
			'-larrow',
			'-larrow_io',
		  ],
		}]
	  ]
	}
  ]
}
