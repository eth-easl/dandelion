cmake_minimum_required(VERSION ${CMAKE_VERSION})

project(Server)

set(EXECUTABLE "server")

add_executable(${EXECUTABLE}
  server.c
)
target_compile_options(${EXECUTABLE} PRIVATE ${PURECAP_COMPILE_FLAGS})
target_link_options(${EXECUTABLE} PRIVATE ${PURECAP_LINK_FLAGS})
