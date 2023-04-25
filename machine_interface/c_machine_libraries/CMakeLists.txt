SET(CMAKE_VERSION 3.13)
cmake_minimum_required(VERSION ${CMAKE_VERSION})

# prevent in-source builds
if(${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
  message(FATAL_ERROR "In-source build detected")
endif()

project(c_machine_libraries LANGUAGES C ASM)

# set path to the compiler to be used
if(DEFINED CMAKE_TOOLCHAIN_FILE OR ${CMAKE_SYSTEM_PROCESSOR} STREQUAL "aarch64c")
  message(STATUS "Compiling with toolchain file or native")
  # compile library instead of executable
  # because cross compiled code can't be executed
  set(CMAKE_TRY_COMPILE_TARGET_TYPE "STATIC_LIBRARY")

  # set flag combinations for use on different targets
  set(PURECAP_COMPILE_FLAGS -march=morello+c64 -mabi=purecap -O3)
  set(PURECAP_LINK_FLAGS -march=morello+c64 -mabi=purecap -fuse-ld=lld)
  set(NOCAP_COMPILE_FLAGS -march=morello -mabi=aapcs -O3)
  set(NOCAP_LINK_FLAGS -march=morello -mabi=aapcs -fuse-ld=lld)

  set(CMAKE_ASM_FLAGS "${CFLAGS} -x assembler-with-cpp")
else()
  message(WARNING "No toolchainfile given, compiling in native mode")
endif()

enable_testing()

add_subdirectory(cheri)
add_subdirectory(unity)