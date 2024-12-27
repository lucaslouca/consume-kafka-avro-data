CMAKE_MINIMUM_REQUIRED(VERSION 3.10)

########################################################################
# Versioning
########################################################################

SET(CCF_MAJOR_VERSION 0)
SET(CCF_MINOR_VERSION 1)
SET(CCF_PATCH_VERSION 0)

SET(CCF_VERSION ${CCF_MAJOR_VERSION}.${CCF_MINOR_VERSION}.${CCF_PATCH_VERSION})

PROJECT(ingest
    VERSION
        ${CCF_VERSION}
    DESCRIPTION
        "ingest"
    LANGUAGES
        CXX
)

MESSAGE(STATUS "CMAKE_ROOT:           ${CMAKE_ROOT}")
MESSAGE(STATUS "CMAKE_SYSTEM_VERSION: ${CMAKE_SYSTEM_VERSION}")

########################################################################
# Macros and additional flags
########################################################################

INCLUDE( CheckCXXCompilerFlag )

FILE( GLOB filenames "cmake/*.cmake" )
FOREACH( filename ${filenames} )
  INCLUDE( ${filename} )
ENDFOREACH()

########################################################################
# APP
########################################################################

ADD_SUBDIRECTORY(src)