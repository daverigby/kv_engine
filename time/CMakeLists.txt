add_static_library_with_pch(mcd_time
            isotime.cc
            ${PROJECT_SOURCE_DIR}/include/memcached/isotime.h)
set_property(TARGET mcd_time PROPERTY POSITION_INDEPENDENT_CODE 1)
TARGET_LINK_LIBRARIES(mcd_time platform)

if (COUCHBASE_KV_BUILD_UNIT_TESTS)
    add_executable_with_pch(memcached_timestamp_test timestamp_test.cc
                   ${PROJECT_SOURCE_DIR}/include/memcached/isotime.h)
    TARGET_LINK_LIBRARIES(memcached_timestamp_test mcd_time)
    ADD_TEST(NAME memcached-timestamp-test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND memcached_timestamp_test)
endif (COUCHBASE_KV_BUILD_UNIT_TESTS)
