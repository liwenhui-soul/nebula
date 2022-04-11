# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        license_test
    SOURCES
        LicenseTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:wkt_wkb_io_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:fs_obj>
        $<TARGET_OBJECTS:encryption_obj>
        # mac addr
        $<TARGET_OBJECTS:network_obj>
        
    LIBRARIES
        gtest
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
)
