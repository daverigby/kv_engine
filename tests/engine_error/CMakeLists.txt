INCLUDE_DIRECTORIES(AFTER SYSTEM ${gtest_SOURCE_DIR}/include)
add_executable_with_pch(memcached-engine-error-test engine_error_test.cc)
TARGET_LINK_LIBRARIES(memcached-engine-error-test engine_utilities gtest gtest_main)
ADD_TEST(NAME memcached-engine-error-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached-engine-error-test)
