cmake_minimum_required(VERSION ${CMAKE_VERSION})

set(LIBRARY "libHTTP")

# target for compartment library
add_library(${LIBRARY} STATIC
  http/mongoose.c
)
# export include directory
target_include_directories(${LIBRARY} PUBLIC http)
target_compile_options(${LIBRARY} PRIVATE ${NOCAP_COMPILE_FLAGS})
target_link_options(${LIBRARY} PRIVATE ${NOCAP_LINK_FLAGS})
target_link_libraries(${LIBRARY} PRIVATE pthread)
set_target_properties(${LIBRARY} PROPERTIES LINKER_LANGUAGE C)