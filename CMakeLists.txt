SET(CMAKE_VERSION 3.13)
cmake_minimum_required(VERSION ${CMAKE_VERSION})

# prevent in-source builds
if(${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
  message(FATAL_ERROR "In-source build detected")
endif()

# set path to the compiler to be used
if(NOT DEFINED CMAKE_TOOLCHAIN_FILE)
  message(ERROR "No toolchainfile given, compiling in native mode")
else()
  # compile library instead of executable
  # because cross compiled code can't be executed
  set(CMAKE_TRY_COMPILE_TARGET_TYPE "STATIC_LIBRARY")
  # clang tripple:
  set(tripple aarch64-unknown-freebsd13)
  set(CMAKE_C_COMPILER_TARGET ${triple})
  set(CMAKE_CXX_COMPILER_TARGET ${triple})
  # set flag combinations for use on different targets
  set(PURECAP_COMPILE_FLAGS -march=morello+c64 -mabi=purecap)
  set(PURECAP_LINK_FLAGS -march=morello+c64 -mabi=purecap -fuse-ld=lld)
  set(NOCAP_COMPILE_FLAGS -march=morello)
  set(NOCAP_LINK_FLAGS -march=morello)
endif()

project(dandelion LANGUAGES C)

# add main server directory
add_subdirectory(server)
# add function libaries used for testing
add_subdirectory(functions)
# compartment setup and execution codes
add_subdirectory(compartment)
# testing framework
add_subdirectory(unity)

# enable testing everythere in the project
enable_testing()
