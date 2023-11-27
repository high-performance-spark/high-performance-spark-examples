################################################################
# A minimal CMake file that is compatible with sbt-jni         #
#                                                              #
# All settings required by sbt-jni have been marked so, please #
# add/modify/remove settings to build your specific library.   #
################################################################

cmake_minimum_required(VERSION 3.12)

option(SBT "Set if invoked from sbt-jni" OFF)

# Define project and related variables
# (required by sbt-jni) please use semantic versioning
#
project (high-performance-spark)
enable_language(Fortran)
set(PROJECT_VERSION_MAJOR 0)
set(PROJECT_VERSION_MINOR 0)
set(PROJECT_VERSION_PATCH 0)

set (LIB_NAME ${PROJECT_NAME}${PROJECT_VERSION_MAJOR})
set (GLUTEN_LIB_NAME ${PROJECT_NAME}-gluten-${PROJECT_VERSION_MAJOR})

# For gluten+velox, you can leave out if not using gluten
set(GLUTEN_HOME ../../gluten)
set(CMAKE_FIND_DEBUG_MODE TRUE)
find_library(VELOX_LIBRARY NAMES velox HINTS
	     ${GLUTEN_HOME}/cpp/build/releases NO_DEFAULT_PATH)
# End gluten specific

if(VELOX_LIBRARY)
    file(GLOB GLUTEN_UDF_FILES
      "./c/gluten/*.cpp")
    add_library(${GLUTEN_LIB_NAME} SHARED ${GLUTEN_UDF_FILES})
    target_include_directories(${GLUTEN_LIB_NAME} PRIVATE ${GLUTEN_HOME}/cpp ${GLUTEN_HOME}/ep/build-velox/build/velox_ep)
    target_link_libraries(${GLUTEN_LIB_NAME} PRIVATE ${VELOX_LIBRARY})
else()
    message(WARNING "Velox library not found. Specific path not added.")
endif()

# Setup JNI
find_package(JNI REQUIRED)
if (JNI_FOUND)
    message (STATUS "JNI include directories: ${JNI_INCLUDE_DIRS}")
endif()

# Include directories
include_directories(.)
include_directories(include)
include_directories(${JNI_INCLUDE_DIRS})

# Sources
file(GLOB LIB_SRC
  "*.c"
  "*.f95"
  "*.f*"
  "*.cc"
  "*.cpp"
  "./c/*.c"
  "./c/*.cpp"
  "./fortran/*.f95"
  "./fortran/*.f*"
)

# Setup installation targets
# (required by sbt-jni) major version should always be appended to library name
#
add_library(${LIB_NAME} SHARED ${LIB_SRC})
install(TARGETS ${LIB_NAME} LIBRARY DESTINATION .)
