project(HipInterface)

cmake_minimum_required(VERSION 3.3)

# Taken from https://github.com/ROCm/hip-tests/blob/rocm-5.5.x/samples/2_Cookbook/15_static_library/host_functions/CMakeLists.txt

if (NOT DEFINED ROCM_PATH )
     set ( ROCM_PATH "/opt/rocm"  CACHE STRING "Default ROCM installation directory." )
endif ()

# Search for rocm in common locations
list(APPEND CMAKE_PREFIX_PATH ${ROCM_PATH}/hip ${ROCM_PATH})

# Find hip
find_package(hip REQUIRED)

# For windows, AR is MS Librarian and that is pickedby Visual Studio's command prompt.
if (WIN32)
  find_program(libpath NAMES lib.exe)
  set (CMAKE_AR ${libpath})
endif()

# Set compiler and linker
set(CMAKE_CXX_COMPILER ${HIP_HIPCC_EXECUTABLE})
set(CMAKE_CXX_LINKER   ${HIP_HIPCC_EXECUTABLE})
set(CMAKE_BUILD_TYPE Release)

# Turn static library generation ON
option(BUILD_SHARED_LIBS "Build as a shared library" OFF)

set(CPP_SOURCES hip_interface.hip.cpp)

# For windows, We need to tell cmake how to create static library.
if (WIN32)
  set (CMAKE_CXX_CREATE_STATIC_LIBRARY "<CMAKE_AR> /out:<TARGET> <LINK_FLAGS> <OBJECTS>")
endif()

# Generate static lib hip_interface_lib.a.
add_library(hip_interface_lib STATIC ${CPP_SOURCES})

# Set-up the correct flags to generate the static library.
target_link_libraries(hip_interface_lib PRIVATE --emit-static-lib)
target_include_directories(hip_interface_lib PRIVATE /opt/rocm/hsa/include)

install(TARGETS ${LIBRARY} DESTINATION ${CMAKE_INSTALL_PREFIX})