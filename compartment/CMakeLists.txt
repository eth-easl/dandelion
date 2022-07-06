cmake_minimum_required(VERSION ${CMAKE_VERSION})

project(Compartment)

set(EXECUTABLE "trial")

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
