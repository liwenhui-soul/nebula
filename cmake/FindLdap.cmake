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
#  Ldap_LIBRARIES        The Ldap libraries.

find_path(Ldap_INCLUDE_DIR NAMES ldap.h)
find_library(Ldap_LIBRARIES NAMES ldap lber)

if(Ldap_INCLUDE_DIR AND Ldap_LIBRARIES)
    set(Ldap_FOUND TRUE)
    mark_as_advanced(
        Ldap_INCLUDE_DIR
        Ldap_LIBRARIES
    )
endif()

if(NOT Ldap_FOUND)
    message(FATAL_ERROR "Ldap doesn't exist")
else()
    message(STATUS "Found LDAP libraries: " ${Ldap_LIBRARIES})
endif()
