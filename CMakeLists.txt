SET(CMAKE_VERSION 3.13)
cmake_minimum_required(VERSION ${CMAKE_VERSION})

# prevent in-source builds
if(${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
  message(FATAL_ERROR "In-source build detected")
endif()

project(dandelion LANGUAGES C ASM)

# set path to the compiler to be used
if(DEFINED CMAKE_TOOLCHAIN_FILE OR ${CMAKE_SYSTEM_PROCESSOR} STREQUAL "aarch64c")
  message(STATUS "Compiling with toolchain file or native")
  # compile library instead of executable
  # because cross compiled code can't be executed
  set(CMAKE_TRY_COMPILE_TARGET_TYPE "STATIC_LIBRARY")

  # set flag combinations for use on different targets
  set(PURECAP_COMPILE_FLAGS -march=morello+c64 -mabi=purecap)
  set(PURECAP_LINK_FLAGS -march=morello+c64 -mabi=purecap -fuse-ld=lld)
  set(NOCAP_COMPILE_FLAGS -march=morello -mabi=aapcs)
  set(NOCAP_LINK_FLAGS -march=morello -mabi=aapcs -fuse-ld=lld)

  set(CMAKE_ASM_FLAGS "${CFLAGS} -x assembler-with-cpp")
else()
  message(WARNING "No toolchainfile given, compiling in native mode")
endif()

# compartment setup and execution code
add_subdirectory(compartment)
# libraries exposed to functions
add_subdirectory(functionInterface)
# functions used for testing
add_subdirectory(functions)
# function management libraries
add_subdirectory(management)
# libary for http server
add_subdirectory(net)
# add main server directory
add_subdirectory(server)
# testing framework
add_subdirectory(unity)

# enable testing everythere in the project
enable_testing()
