cmake_minimum_required(VERSION 2.6)

# Define project and related variables
#
project (high-performance-spark)

# Set versions and library name
# Note: major version will be appended to library name
#
set (VERSION_MAJOR 0)
set (VERSION_MINOR 1)
set (VERSION_PATCH 0)
set (LIB_NAME flow${VERSION_MAJOR})

# Command-line options
#
# required by sbt-jni to install binaries to correct places
set (LIB_INSTALL_DIR lib CACHE PATH "Path in which to install libraries (Autoconf equivalent to --libdir).")
# required by sbt-jni to disable versioned libraries
set (ENABLE_VERSIONED_LIB ON CACHE BOOLEAN "Generate versioned library files and symlinks.")

# Setup JNI
find_package(JNI REQUIRED)
if (JNI_FOUND)
    message (STATUS "JNI include directories: ${JNI_INCLUDE_DIRS}")
endif()

# Include directories
include_directories(include)
include_directories(${JNI_INCLUDE_DIRS})

# Setup main shared library
# Note: major version is appended to library name
add_library(${LIB_NAME} SHARED main/c/sum.c main/c/sum_wrapper.c)
if (ENABLE_VERSIONED_LIB)
   set_target_properties(
	${LIB_NAME}
	PROPERTIES
	VERSION 0.${VERSION_MINOR}.${VERSION_PATCH} # major version always 0, it is included in name
	SOVERSION 0
   )
endif()

# Installation targets
install(TARGETS ${LIB_NAME} LIBRARY DESTINATION ${LIB_INSTALL_DIR}) 
