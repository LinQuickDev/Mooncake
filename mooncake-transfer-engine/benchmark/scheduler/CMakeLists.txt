cmake_minimum_required(VERSION 3.16)
project(mooncake-scheduler-benchmark LANGUAGES CXX)

set(BUILD_BENCHMARK OFF CACHE BOOL "" FORCE)
set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(BUILD_UNIT_TESTS OFF CACHE BOOL "" FORCE)

add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/../..
                 ${CMAKE_CURRENT_BINARY_DIR}/transfer-engine)

add_executable(scheduler_benchmark scheduler_benchmark.cpp)
target_compile_features(scheduler_benchmark PRIVATE cxx_std_20)
target_include_directories(
  scheduler_benchmark PRIVATE ${CMAKE_CURRENT_LIST_DIR}/../../include)
target_link_libraries(scheduler_benchmark PRIVATE transfer_engine)

configure_file(compare_results.py compare_results.py COPYONLY)
