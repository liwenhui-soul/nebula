# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
 
 nebula_add_executable(
    NAME
        machine-code-generator
    SOURCES
        machineCodeGeneratorTool.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:fs_obj>
        $<TARGET_OBJECTS:encryption_obj>
        $<TARGET_OBJECTS:network_obj>
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
)

install(
    TARGETS
        machine-code-generator
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        bin
    COMPONENT
        tool
)
