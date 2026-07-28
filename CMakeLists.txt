cmake_minimum_required(VERSION 3.15)
project(ThreadPool CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

add_executable(test_phase1 tests/test_phase1.cpp)
target_include_directories(test_phase1 PRIVATE include)
target_link_libraries(test_phase1 PRIVATE Threads::Threads)