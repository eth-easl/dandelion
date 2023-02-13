cmake_minimum_required(VERSION ${CMAKE_VERSION})

set(LIBRARY "compartmentLib")

# target for compartment library
add_library(${LIBRARY} STATIC
  src/compartment.c
  src/context.h
)
# export include directory
target_include_directories(${LIBRARY} PUBLIC include)
target_compile_options(${LIBRARY} PRIVATE ${NOCAP_COMPILE_FLAGS})
target_link_options(${LIBRARY} PRIVATE ${NOCAP_LINK_FLAGS})
target_link_libraries(${LIBRARY} PRIVATE pthread)

# add testing directory
add_subdirectory(test)

# add trial subdirectory TODO remove when not needed anymore
add_subdirectory(trial)
