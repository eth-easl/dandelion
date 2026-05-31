cmake_minimum_required(VERSION 3.23)
project(my_first_program LANGUAGES C)

# Point this to the pre-installed SDK
add_subdirectory(/home/lmo/dandelionSDK/dev/.dlibc-dev/dandelion_sdk dandelion_sdk)

add_executable(echo echo.c)
target_compile_options(echo PRIVATE -fno-stack-protector -O0)
target_link_options(echo PRIVATE -static)
target_link_libraries(echo PRIVATE dandelion_runtime)

add_executable(test_string test_string.c)
target_compile_options(test_string PRIVATE -fno-stack-protector -O0)
target_link_options(test_string PRIVATE -static)
target_link_libraries(test_string PRIVATE dandelion_runtime)
