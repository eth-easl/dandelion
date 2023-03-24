cmake_minimum_required(VERSION ${CMAKE_VERSION})

# set library name
set(LIBRARY "cheri_mem")
add_library(${LIBRARY} STATIC 
    src/memory.c
    src/cpu.c
)
target_include_directories(${LIBRARY} PUBLIC include)
target_compile_options(${LIBRARY} PRIVATE ${NOCAP_COMPILE_FLAGS})
target_link_options(${LIBRARY} PRIVATE ${NOCAP_LINK_FLAGS})

# install rules
install(TARGETS ${LIBRARY} DESTINATION ${CMAKE_INSTALL_PREFIX})

# set tests
set(TEST cheri_test)
add_executable(${TEST}
    test/test.c
)
target_link_libraries(${TEST}
    ${LIBRARY}
    unityHybridCap
)
target_compile_options(${TEST} PRIVATE ${NOCAP_COMPILE_FLAGS})
target_link_options(${TEST} PRIVATE ${NOCAP_LINK_FLAGS})
# register test
add_test(${TEST} ${TEST})
