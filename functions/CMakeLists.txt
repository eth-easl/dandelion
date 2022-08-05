cmake_minimum_required(VERSION ${CMAKE_VERSION})

set(LIBRARY_BASIC "basic")

add_library(${LIBRARY_BASIC} SHARED
  basic/basic.c
)
target_include_directories(${LIBRARY_BASIC} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/basic)
# compile options, set O0 to make sure the code is not unexpectedly optimized
target_compile_options(${LIBRARY_BASIC} PRIVATE ${NOCAP_COMPILE_FLAGS} -O0)
target_link_options(${LIBRARY_BASIC} PRIVATE ${NOCAP_LINK_FLAGS})

set(EXECUTABLE "wrapper")
add_executable(${EXECUTABLE}
  wrapper/wrapperMain.c
)

target_compile_options(${EXECUTABLE} PRIVATE ${NOCAP_COMPILE_FLAGS} -static -O2)
target_link_options(${EXECUTABLE} PRIVATE ${NOCAP_LINK_FLAGS} -static -fno-builtin -nostdlib -e main)
# target_link_libraries(${EXECUTABLE} PRIVATE ioLib)
