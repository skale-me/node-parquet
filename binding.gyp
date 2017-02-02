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
	  'libraries': [
	  	'-Wl,-rpath,build_deps/parquet-cpp/release',
		'-L../build_deps/parquet-cpp/release',
		'-lparquet',
		'-Wl,-rpath,build_deps/parquet-cpp/arrow_ep/src/arrow_ep-install/lib',
	  ],
	  'cflags!': [ '-fno-exceptions' ],
	  'cflags_cc!': [ '-fno-exceptions' ],
	  'conditions': [
	  	['OS=="mac"', {
		  'xcode_settings': {
		   'GCC_ENABLE_CPP_EXCEPTIONS': 'YES'
		  }
		}]
	  ]
	}
  ]
}
