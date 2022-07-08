cmake_minimum_required(VERSION ${CMAKE_VERSION})

# name the test
set(TEST compartmentTest)

# add an executable for the test
add_executable(${TEST}
  test.c
)

# include libraries
target_link_libraries(${TEST}
  # unit under test, should be defined in parent cmake file
  ${LIBRARY}
  # test library
  unityHybridCap
)

# add exeutable as test
add_test(${TEST} ${TEST})
