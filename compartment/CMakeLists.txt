cmake_minimum_required(VERSION ${CMAKE_VERSION})

set(EXECUTABLE "trial")
set(LIBRARY "compartmentLib")
set(MEASURE_TEST "cycletest")

add_executable(${EXECUTABLE}
  trial.c
)

target_compile_options(${EXECUTABLE} PRIVATE ${NOCAP_COMPILE_FLAGS} -O0)
target_link_options(${EXECUTABLE} PRIVATE ${NOCAP_LINK_FLAGS})
target_link_libraries(${EXECUTABLE} PRIVATE ${LIBRARY})

add_executable(${MEASURE_TEST}
  measurementTest.c
)
target_compile_options(${MEASURE_TEST} PRIVATE ${NOCAP_COMPILE_FLAGS} -O2)
target_link_options(${MEASURE_TEST} PRIVATE ${NOCAP_LINK_FLAGS})

# target for compartment library
add_library(${LIBRARY} STATIC
  src/assembly.asm
  src/compartment.c
  src/context.h
)
# export include directory
target_include_directories(${LIBRARY} PUBLIC include)
target_compile_options(${LIBRARY} PRIVATE ${NOCAP_COMPILE_FLAGS})
target_link_options(${LIBRARY} PRIVATE ${NOCAP_LINK_FLAGS})

# add testing directory
add_subdirectory(test)
