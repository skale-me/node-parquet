{
  'targets': [
    {
	  'target_name': 'parquet',
	  'sources': [
	  	'src/parquet_binding.cc',
	  	'src/parquet_reader.cc',
	  ],
	  'include_dirs': [
      "deps/parquet-cpp/src",
	  "build_deps/parquet-cpp/thirdparty/installed/include",
      "<!(node -e \"require('nan')\")"
    ],
	  'libraries': [
	  	'-Wl,-rpath,build_deps/parquet-cpp/release',
		'-L../build_deps/parquet-cpp/release',
		'-lparquet',
		'-Wl,-rpath,build_deps/parquet-cpp/thirdparty/installed/lib',
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
