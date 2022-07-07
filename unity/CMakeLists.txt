cmake_minimum_required(VERSION ${CMAKE_VERSION})

set(UNITY_CAPS unityFullCap)
set(UNITY_HYPBRID unityHybridCap)
set(SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/source/src)

# test library for full caps
add_library(${UNITY_CAPS} STATIC
  ${SOURCE_DIR}/unity.c
)
target_include_directories(${UNITY_CAPS} PUBLIC
  ${SOURCE_DIR}
)
target_compile_options(${UNITY_CAPS} PRIVATE ${PURECAP_COMPILE_FLAGS})
target_link_options(${UNITY_CAPS} PRIVATE ${PURECAP_LINK_FLAGS})

# test library for hybrid caps
add_library(${UNITY_HYPBRID} STATIC
  ${SOURCE_DIR}/unity.c
)
target_include_directories(${UNITY_HYPBRID} PUBLIC
  ${SOURCE_DIR}
)
target_compile_options(${UNITY_HYPBRID} PRIVATE ${NOCAP_COMPILE_FLAGS})
target_link_options(${UNITY_HYPBRID} PRIVATE ${NOCAP_LINK_FLAGS})
