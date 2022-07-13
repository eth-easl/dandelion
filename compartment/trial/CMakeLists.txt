cmake_minimum_required(VERSION ${CMAKE_VERSION})

set(EXECUTABLE "trial")
set(MEASURE_TEST "cycletest")
add_executable(${EXECUTABLE}
  trial.c
  trial.asm
)

target_compile_options(${EXECUTABLE} PRIVATE ${NOCAP_COMPILE_FLAGS} -O0)
target_link_options(${EXECUTABLE} PRIVATE ${NOCAP_LINK_FLAGS})
target_link_libraries(${EXECUTABLE} PRIVATE ${LIBRARY})

add_executable(${MEASURE_TEST}
  measurementTest.c
)
target_compile_options(${MEASURE_TEST} PRIVATE ${NOCAP_COMPILE_FLAGS} -O2)
target_link_options(${MEASURE_TEST} PRIVATE ${NOCAP_LINK_FLAGS})
