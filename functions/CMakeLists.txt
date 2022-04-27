cmake_minimum_required(VERSION ${CMAKE_VERSION})

project(FunctionLibs)

set(LIBRARY_BASIC "basic")

add_library(${LIBRARY_BASIC} SHARED
  basic/basic.c
)
# compile options, set O0 to make sure the code is not unexpectedly optimized
target_compile_options(${LIBRARY_BASIC} PRIVATE ${NOCAP_COMPILE_FLAGS} -O0)
target_link_options(${LIBRARY_BASIC} PRIVATE ${NOCAP_LINK_FLAGS})
