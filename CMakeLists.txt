cmake_minimum_required(VERSION 3.15)
project(ThreadPool CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Default to Debug so asserts stay live unless someone explicitly asks otherwise.
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

find_package(Threads REQUIRED)

# The pool is header-only, so it becomes an INTERFACE target:
# no compiled output, just include paths + link requirements that
# propagate to anything that uses it.
add_library(threadpool INTERFACE)
target_include_directories(threadpool INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(threadpool INTERFACE Threads::Threads)
target_compile_features(threadpool INTERFACE cxx_std_17)

add_executable(test_phase1 tests/test_phase1.cpp)
target_link_libraries(test_phase1 PRIVATE threadpool)
target_compile_options(test_phase1 PRIVATE -Wall -Wextra -Wpedantic)