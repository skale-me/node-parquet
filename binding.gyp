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
      "build_deps/parquet-cpp/release/include",
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
      '-w',
      '<(module_root_dir)/build_deps/parquet-cpp/release/libparquet.a',
      '<(module_root_dir)/build_deps/parquet-cpp/release/libarrow.a',
      '<(module_root_dir)/build_deps/parquet-cpp/thrift_ep/src/thrift_ep-install/lib/libthrift.a',
      '-lboost_regex',
      ],
    },
    'OS=="linux"', {
      'libraries': [
      '<(module_root_dir)/build_deps/parquet-cpp/release/libparquet.a',
      '<(module_root_dir)/build_deps/parquet-cpp/release/libarrow.a',
      '<(module_root_dir)/build_deps/parquet-cpp/thrift_ep/src/thrift_ep-install/lib/libthrift.a',
      '-lboost_regex',
      ],
    }]
    ]
  }
  ]
}
