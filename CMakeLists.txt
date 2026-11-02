cmake_minimum_required(VERSION 3.16 FATAL_ERROR)
project(ton-librarian CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

set(TON_SOURCE_DIR "" CACHE PATH "Path to ton-blockchain/ton checkout (default: submodule at external/ton)")
if (TON_SOURCE_DIR STREQUAL "")
  set(TON_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external/ton")
endif()
if (NOT EXISTS "${TON_SOURCE_DIR}/CMakeLists.txt")
  message(FATAL_ERROR "ton sources not found at ${TON_SOURCE_DIR}; run: git submodule update --init --recursive")
endif()
message(STATUS "ton-librarian: using ton sources at ${TON_SOURCE_DIR}")

# ton requires several CMake variables to be set before add_subdirectory
set(USE_GMOCK OFF CACHE BOOL "" FORCE)
set(TON_USE_ROCKSDB ON CACHE BOOL "" FORCE)
set(TDDB_USE_ROCKSDB ON CACHE BOOL "" FORCE)
set(TON_ONLY_TONLIB OFF CACHE BOOL "" FORCE)
add_subdirectory("${TON_SOURCE_DIR}" ton-build EXCLUDE_FROM_ALL)

add_subdirectory(src)
