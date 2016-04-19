project(sframe)

 make_library(sframe
   SOURCES
     dataframe.cpp
     sarray_index_file.cpp
     sframe_constants.cpp
     sframe_config.cpp
     sframe.cpp
     sframe_reader.cpp
     sframe_index_file.cpp
     parallel_csv_parser.cpp
     sframe_io.cpp
     shuffle.cpp
     csv_line_tokenizer.cpp
     sarray_v1_block_manager.cpp
     sarray_v2_block_manager.cpp
     sarray_v2_type_encoding.cpp
     sarray_v2_block_writer.cpp
     sarray_sorted_buffer.cpp
     sarray_v2_encoded_block.cpp
     groupby.cpp
     groupby_aggregate.cpp
     groupby_aggregate_impl.cpp
     group_aggregate_value.cpp
     sframe_iterators.cpp
     csv_writer.cpp
     join.cpp
     join_impl.cpp
     unfair_lock.cpp
     sframe_rows.cpp
     generic_avro_reader.cpp
     odbc_connector.cpp
     libodbc_shim.cpp
     testing_utils.cpp
     sframe_saving.cpp
     sframe_saving_impl.cpp
     rolling_aggregate.cpp
   REQUIRES
     random flexible_type fileio parallel lz4 
     cancel_serverside_ops serialization libjson globals avrocpp odbc
    EXTERNAL_VISIBILITY
 )

make_copy_target(_local_sys_util_
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/../unity/python/sframe/sys_util.py)

make_library(spark_unity
SOURCES
  spark_unity.cpp
REQUIRES
  flexible_type sframe fileio python_callbacks 
)

file(DOWNLOAD http://s3-us-west-2.amazonaws.com/glbin-engine/spark_unity_0.7.jar ${CMAKE_CURRENT_BINARY_DIR}/spark_unity.jar
      EXPECTED_MD5 92d0078d8382eb6e4b1bde8f13db6006)


