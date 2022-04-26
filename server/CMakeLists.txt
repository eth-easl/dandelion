cmake_minimum_required(VERSION ${CMAKE_VERSION})

project(Server)

set(EXECUTABLE "server")

add_executable(${EXECUTABLE}
  server.c
)
target_compile_options(${EXECUTABLE} PRIVATE -march=morello+c64 -mabi=purecap)
