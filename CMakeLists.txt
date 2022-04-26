SET(CMAKE_VERSION 3.10)
cmake_minimum_required(VERSION ${CMAKE_VERSION})

# prevent in-source builds
if(${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
  message(FATAL_ERROR "In-source build detected")
endif()

# set path to the compiler to be used
if(NOT DEFINED CMAKE_TOOLCHAIN_FILE)
  message(WARNING "No toolchainfile given, compiling in native mode")
else()
  # compile library instead of executable
  # because cross compiled code can't be executed
  set(CMAKE_TRY_COMPILE_TARGET_TYPE "STATIC_LIBRARY")
  # clang tripple:
  set(tripple aarch64-unknown-freebsd13)
  set(CMAKE_C_COMPILER_TARGET ${triple})
  set(CMAKE_CXX_COMPILER_TARGET ${triple})
  # enable morello architecture
  set(CMAKE_EXE_LINKER_FLAGS_INIT "-march=morello+c64 -mabi=purecap -fuse-ld=lld")
  set(CMAKE_MODULE_LINKER_FLAGS_INIT "-march=morello+c64 -mabi=purecap -fuse-ld=lld")
  set(CMAKE_SHARED_LINKER_FLAGS_INIT "-march=morello+c64 -mabi=purecap -fuse-ld=lld")
endif()

project(dandelion)

# add main server directory
add_subdirectory(server)
# add function libaries used for testing
add_subdirectory(functions)
