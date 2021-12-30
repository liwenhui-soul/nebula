# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    audit_obj OBJECT
    AuditLogger.cpp
    AuditLogging.cpp
    AuditFilter.cpp
    AuditFormat.cpp
    AuditHandler.cpp
    ESAuditHandler.cpp
)

nebula_add_subdirectory(test)
