cmake_minimum_required(VERSION ${CMAKE_VERSION})

# name the test
set(TEST compartmentTest)

# add an executable for the test
add_executable(${TEST}
  test.c
  testFunctions.asm
)

# include libraries
target_link_libraries(${TEST}
  # unit under test, should be defined in parent cmake file
  ${LIBRARY}
  # test library
  unityHybridCap
)
target_compile_options(${TEST} PRIVATE ${NOCAP_COMPILE_FLAGS})
target_link_options(${TEST} PRIVATE ${NOCAP_LINK_FLAGS})

# add exeutable as test
add_test(${TEST} ${TEST})
