# - Try to find Ldap includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Ldap)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Ldap_FOUND            System has Ldap, include and lib dirs found
#  Ldap_INCLUDE_DIR      The Ldap includes directories.
#  Ldap_LIBRARY          The Ldap library.

find_path(Ldap_INCLUDE_DIR NAMES ldap.h)
find_library(Ldap_LIBRARY NAMES ldap)

if(Ldap_INCLUDE_DIR AND Ldap_LIBRARY)
    set(Ldap_FOUND TRUE)
    mark_as_advanced(
        Ldap_INCLUDE_DIR
        Ldap_LIBRARY
    )
endif()

if(NOT Ldap_FOUND)
    message(FATAL_ERROR "Ldap doesn't exist")
endif()
