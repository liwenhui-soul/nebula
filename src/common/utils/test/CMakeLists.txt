nebula_add_test(
    NAME
        nebula_key_utils_test
    SOURCES
        NebulaKeyUtilsTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:keyutils_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:wkt_wkb_io_obj>
        $<TARGET_OBJECTS:meta_thrift_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME
        index_key_utils_test
    SOURCES
        IndexKeyUtilsTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:keyutils_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:wkt_wkb_io_obj>
        $<TARGET_OBJECTS:meta_thrift_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME
        operation_key_utils_test
    SOURCES
        OperationKeyUtilsTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:keyutils_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:wkt_wkb_io_obj>
        $<TARGET_OBJECTS:meta_thrift_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)
