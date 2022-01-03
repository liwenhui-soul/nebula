# - Try to find BerkeleyDB includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(BerkeleyDB)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Bdb_FOUND             System has BerkeleyDB, include and lib dirs found
#  Bdb_INCLUDE_DIR       The Berkeley includes directories.
#  Bdb_LIBRARIES         The Berkeley libraries.

# We are not looking for db.h here, in order to avoid conflicts with rocksdb/db.h
find_path(Bdb_INCLUDE_DIR
          NAMES db_cxx.h
          PATH_SUFFIXES berkeleydb-5.1.29)
find_library(Bdb_LIBRARIES
             NAMES db-5.1
             PATH_SUFFIXES berkeleydb-5.1.29)

if(Bdb_INCLUDE_DIR AND Bdb_LIBRARIES)
    set(Bdb_FOUND TRUE)
    mark_as_advanced(
        Bdb_INCLUDE_DIR
        Bdb_LIBRARIES
    )
endif()

if(NOT Bdb_FOUND)
    message(FATAL_ERROR "BerkeleyDB doesn't exist")
else()
    message(STATUS "Found BerkeleyDB libraries: " ${Bdb_LIBRARIES})
endif()
