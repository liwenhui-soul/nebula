# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

set(ws_test_deps
    $<TARGET_OBJECTS:http_client_obj>
    $<TARGET_OBJECTS:ws_obj>
    $<TARGET_OBJECTS:ws_common_obj>
    $<TARGET_OBJECTS:stats_obj>
    $<TARGET_OBJECTS:datatypes_obj>
    $<TARGET_OBJECTS:process_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:version_obj>
    $<TARGET_OBJECTS:wkt_wkb_io_obj>
    $<TARGET_OBJECTS:encryption_obj>
    $<TARGET_OBJECTS:network_obj>
)

nebula_add_test(
    NAME
        flags_access_test
    SOURCES
        FlagsAccessTest.cpp
    OBJECTS
        ${ws_test_deps}
    LIBRARIES
        ${PROXYGEN_LIBRARIES}
        ${ROCKSDB_LIBRARIES}
        gtest
)


nebula_add_test(
    NAME
        stats_read_test
    SOURCES
        StatsReaderTest.cpp
    OBJECTS
        ${ws_test_deps}
    LIBRARIES
        ${PROXYGEN_LIBRARIES}
        ${ROCKSDB_LIBRARIES}
        gtest
)


nebula_add_test(
    NAME
        router_test
    SOURCES
        RouterTest.cpp
    OBJECTS
        ${ws_test_deps}
    LIBRARIES
        ${PROXYGEN_LIBRARIES}
        ${ROCKSDB_LIBRARIES}
        gtest
)

nebula_add_test(
    NAME
        http_status_test
    SOURCES
        StatusHandlerTest.cpp
    OBJECTS
        ${ws_test_deps}
    LIBRARIES
        ${PROXYGEN_LIBRARIES}
        ${ROCKSDB_LIBRARIES}
        gtest
)

nebula_add_test(
    NAME
        http_license_test
    SOURCES
        LicenseHandlerTest.cpp
    OBJECTS
        ${ws_test_deps}
    LIBRARIES
        ${PROXYGEN_LIBRARIES}
        ${ROCKSDB_LIBRARIES}
        gtest
)
