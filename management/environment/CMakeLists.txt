cmake_minimum_required(VERSION ${CMAKE_VERSION})

set(LIBRARY "environment")

# target for compartment library
add_library(${LIBRARY} STATIC
  src/elfParsing.c
  src/functionManagement.c
)
# export include directory
target_include_directories(${LIBRARY} PUBLIC include)
target_compile_options(${LIBRARY} PRIVATE ${NOCAP_COMPILE_FLAGS})
target_link_options(${LIBRARY} PRIVATE ${NOCAP_LINK_FLAGS})
target_link_libraries(${LIBRARY} PRIVATE ioLib compartmentLib)

# add testing directory
add_subdirectory(test)
