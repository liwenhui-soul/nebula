# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        audit_logger_test
    SOURCES
        AuditLoggerTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:audit_obj>
        $<TARGET_OBJECTS:thread_obj>
        $<TARGET_OBJECTS:graph_flags_obj>
        $<TARGET_OBJECTS:version_obj>
    LIBRARIES
        gtest
)

nebula_add_test(
    NAME
        audit_handler_test
    SOURCES
        AuditHandlerTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:audit_obj>
        $<TARGET_OBJECTS:thread_obj>
        $<TARGET_OBJECTS:graph_flags_obj>
        $<TARGET_OBJECTS:version_obj>
    LIBRARIES
        gtest
)

nebula_add_test(
    NAME
        audit_filter_test
    SOURCES
        AuditFilterTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:audit_obj>
        $<TARGET_OBJECTS:thread_obj>
        $<TARGET_OBJECTS:graph_flags_obj>
        $<TARGET_OBJECTS:version_obj>
    LIBRARIES
        gtest
)

nebula_add_test(
    NAME
        audit_format_test
    SOURCES
        AuditFormatTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:audit_obj>
        $<TARGET_OBJECTS:thread_obj>
        $<TARGET_OBJECTS:graph_flags_obj>
        $<TARGET_OBJECTS:version_obj>
    LIBRARIES
        gtest
)

nebula_add_test(
    NAME
        es_audit_handler_test
    SOURCES
        ESAuditHandlerTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:audit_obj>
        $<TARGET_OBJECTS:thread_obj>
        $<TARGET_OBJECTS:graph_flags_obj>
        $<TARGET_OBJECTS:version_obj>
    LIBRARIES
        gtest
)

nebula_add_test(
    NAME
        utility_test
    SOURCES
        UtilityTest.cpp
    LIBRARIES
        gtest
)
