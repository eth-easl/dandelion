cmake_minimum_required(VERSION ${CMAKE_VERSION})

project(Server)

set(EXECUTABLE_NAME "server")

add_executable(${EXECUTABLE_NAME}
  server.c
)
