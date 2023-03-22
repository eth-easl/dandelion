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

# test directory
# add_subdirectory(test)
