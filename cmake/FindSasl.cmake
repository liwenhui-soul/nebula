# - Try to find Sasl includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Sasl)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Sasl_FOUND            System has Sasl, include and lib dirs found
#  Sasl_INCLUDE_DIR      The Sasl includes directories.
#  Sasl_LIBRARIES        The Sasl libraries.

find_path(Sasl_INCLUDE_DIR
          NAMES sasl.h
          PATH_SUFFIXES sasl)
find_library(Sasl_LIBRARIES NAMES sasl2)

if(Sasl_INCLUDE_DIR AND Sasl_LIBRARIES)
    set(Sasl_FOUND TRUE)
    mark_as_advanced(
        Sasl_INCLUDE_DIR
        Sasl_LIBRARIES
    )
endif()

if(NOT Sasl_FOUND)
    message(FATAL_ERROR "Sasl doesn't exist")
else()
    message(STATUS "Found Sasl libraries: " ${Sasl_LIBRARIES})
endif()
