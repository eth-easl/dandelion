cmake_minimum_required(VERSION ${CMAKE_VERSION})

set(EXECUTABLE "trial")
set(LIBRARY "compartmentLib")

add_executable(${EXECUTABLE}
  trial.c
)

target_compile_options(${EXECUTABLE} PRIVATE ${NOCAP_COMPILE_FLAGS} -O0)
target_link_options(${EXECUTABLE} PRIVATE ${NOCAP_LINK_FLAGS})

set(MEASURE_TEST "cycletest")
add_executable(${MEASURE_TEST}
  measurementTest.c
)
target_compile_options(${EXECUTABLE} PRIVATE ${NOCAP_COMPILE_FLAGS} -O2)
target_link_options(${EXECUTABLE} PRIVATE ${NOCAP_LINK_FLAGS})

# target for compartment library
add_library(${LIBRARY} STATIC
  src/compartment.h
)
# export include directory
target_include_directories(${LIBRARY} PUBLIC include)
set_target_properties(${LIBRARY} PROPERTIES LINKER_LANGUAGE C)
target_compile_options(${LIBRARY} PRIVATE ${NOCAP_COMPILE_FLAGS})
target_link_options(${LIBRARY} PRIVATE ${NOCAP_LINK_FLAGS})

# add testing directory
add_subdirectory(test)
