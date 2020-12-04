add_executable_with_pch(memcached_sizes
               sizes.cc)
TARGET_LINK_LIBRARIES(memcached_sizes mcbp memcached_logger)

ADD_TEST(NAME memcached-sizes
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_sizes)
