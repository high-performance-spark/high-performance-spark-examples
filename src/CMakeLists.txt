################################################################
# A minimal CMake file that is compatible with sbt-jni         #
#                                                              #
# All settings required by sbt-jni have been marked so, please #
# add/modify/remove settings to build your specific library.   #
################################################################

cmake_minimum_required(VERSION 2.6)

# Define project and related variables
#
project (high-performance-spark)

# Enable fortan
enable_language (Fortran)
include(FortranCInterface)
include(CMakeAddFortranSubdirectory)
cmake_add_fortran_subdirectory("./main/fortran" PROJECT fortran NO_EXTERNAL_INSTALL)

# FFLAGS depend on the compiler
get_filename_component (Fortran_COMPILER_NAME ${CMAKE_Fortran_COMPILER} NAME)


# Set versions and library name
# (required by sbt-jni) please use semantic versioning
#
set (VERSION_MAJOR 0)
set (VERSION_MINOR 0)
set (VERSION_PATCH 0)
# (required by sbt-jni) major version will always be appended to library name
set (LIB_NAME ${CMAKE_PROJECT_NAME}${VERSION_MAJOR})

# Command-line options
#
# (set by sbt-jni)
set (LIB_INSTALL_DIR lib CACHE PATH "Path in which to install libraries (equivalent to Autoconf --libdir).")
# (set by sbt-jni)
set (LIB_ENABLE_MINOR_VERSIONS ON CACHE BOOLEAN "Build libraries with minor and patch versions appended.")

# Setup JNI
find_package(JNI REQUIRED)
if (JNI_FOUND)
    message (STATUS "JNI include directories: ${JNI_INCLUDE_DIRS}")
endif()

# Include directories
include_directories(.)
include_directories(./main/c)
include_directories(./main/c/include)
include_directories(${JNI_INCLUDE_DIRS})

# Setup main shared library
file(GLOB LIB_SRC
  "*.c"
  "*.cpp"
  "./main/c/*.c"
  "./main/c/*.cpp"
  "./main/fortran/*.f*"
)
add_library(${LIB_NAME} SHARED ${LIB_SRC})

# By default, in a regular build, minor and patch versions are added to the generated files.
# When built through sbt-jni however, LIB_ENABLE_MINOR_VERSIONS is deactivated and only a
# major-versioned library file is built.
if (LIB_ENABLE_MINOR_VERSIONS)
   set_target_properties(
	${LIB_NAME}
	PROPERTIES
	VERSION 0.${VERSION_MINOR}.${VERSION_PATCH} # major version always 0, it is included in library name
	SOVERSION 0
   )
endif()

# Installation targets
install(TARGETS ${LIB_NAME} LIBRARY DESTINATION ${LIB_INSTALL_DIR})
