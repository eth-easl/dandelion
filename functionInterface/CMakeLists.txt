cmake_minimum_required(VERSION ${CMAKE_VERSION})

set(LIBRARY "ioLib")

# target for compartment library
add_library(${LIBRARY} STATIC
  src/dandelionIO.c
)
# export include directory
target_include_directories(${LIBRARY} PUBLIC include)
target_compile_options(${LIBRARY} PRIVATE ${NOCAP_COMPILE_FLAGS})
target_link_options(${LIBRARY} PRIVATE ${NOCAP_LINK_FLAGS})
set_target_properties(${LIBRARY} PROPERTIES LINKER_LANGUAGE C)
