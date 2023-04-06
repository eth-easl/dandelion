cmake_minimum_required(VERSION ${CMAKE_VERSION})

# set library name
set(LIBRARY "cheri_lib")
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
set(MEM_TEST cheri_mem_test)
add_executable(${MEM_TEST}
    test/test.c
)
target_link_libraries(${MEM_TEST}
    ${LIBRARY}
    unityHybridCap
)
target_compile_options(${MEM_TEST} PRIVATE ${NOCAP_COMPILE_FLAGS})
target_link_options(${MEM_TEST} PRIVATE ${NOCAP_LINK_FLAGS})

set(CPU_TEST cheri_cpu_test)
add_executable(${CPU_TEST}
    test/cpu_test.c
    test/testFunctions.asm
)
target_link_libraries(${CPU_TEST}
    ${LIBRARY}
    unityHybridCap
)
target_compile_options(${CPU_TEST} PRIVATE ${NOCAP_COMPILE_FLAGS})
target_link_options(${CPU_TEST} PRIVATE ${NOCAP_LINK_FLAGS})
# register test
add_test(${MEM_TEST} ${MEM_TEST})
add_test(${CPU_TEST} ${CPU_TEST})
