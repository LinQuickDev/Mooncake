# FindUbDiag.cmake v2 — 基于 UBDIAG_DISABLE 的两层集成
#
# Layer 0 (默认): FetchContent 拉源码 + 定义 UBDIAG_DISABLE → constexpr 空函数
# Layer 1 (可选): FetchContent 拉源码 + 编译 ubdiag 库 + CLI
#
# Usage:
#   include(${CMAKE_SOURCE_DIR}/mooncake-common/FindUbDiag.cmake)
#   target_link_libraries(your_target PRIVATE UbDiag::ubdiag_lib)
#
# Options:
#   -DMOONCAKE_ENABLE_UBDIAG=ON   编译真实 ubdiag (库 + CLI)
#   -DMOONCAKE_ENABLE_UBDIAG=OFF  (默认) 使用 UBDIAG_DISABLE 空函数
#   -DMOONCAKE_UBDIAG_GIT_TAG=<tag/branch/commit>  指定 ubdiag 版本
#   -DMOONCAKE_UBDIAG_SOURCE_DIR=/path/to/ubdiag  离线模式指定本地源码

if(TARGET UbDiag::ubdiag_lib)
  return()
endif()

option(MOONCAKE_ENABLE_UBDIAG "编译 ubdiag 真实库(否则用 UBDIAG_DISABLE 空函数)" OFF)
set(MOONCAKE_UBDIAG_GIT_REPOSITORY
    "https://github.com/LinQuickDev/ubdiag.git"
    CACHE STRING "ubdiag Git repository")
set(MOONCAKE_UBDIAG_GIT_TAG
    "705c6c37da45df2be4bc64c134dca0b7f30b2113"
    CACHE STRING "ubdiag 版本(tag/branch/commit)")
set(MOONCAKE_UBDIAG_SOURCE_DIR "" CACHE PATH "本地 ubdiag 源码目录(离线用,为空则 FetchContent)")

include(FetchContent)

# ===== Layer 0: UBDIAG_DISABLE 模式(默认) =====
if(NOT MOONCAKE_ENABLE_UBDIAG)
  # Populate the source tree only. The full FetchContent_Populate() form stays
  # source-only without the deprecated single-argument call on newer CMake.
  if(MOONCAKE_UBDIAG_SOURCE_DIR AND
     EXISTS "${MOONCAKE_UBDIAG_SOURCE_DIR}/CMakeLists.txt")
    set(ubdiag_SOURCE_DIR "${MOONCAKE_UBDIAG_SOURCE_DIR}")
  else()
    FetchContent_Populate(ubdiag
      GIT_REPOSITORY ${MOONCAKE_UBDIAG_GIT_REPOSITORY}
      GIT_TAG ${MOONCAKE_UBDIAG_GIT_TAG}
      SOURCE_DIR "${FETCHCONTENT_BASE_DIR}/ubdiag-src"
      BINARY_DIR "${FETCHCONTENT_BASE_DIR}/ubdiag-build"
      SUBBUILD_DIR "${FETCHCONTENT_BASE_DIR}/ubdiag-subbuild")
  endif()

  set(_MOONCAKE_UBDIAG_PERF_POINT_HEADER
      "${ubdiag_SOURCE_DIR}/include/ubdiag/perf_point.h")
  if(NOT EXISTS "${_MOONCAKE_UBDIAG_PERF_POINT_HEADER}")
    message(FATAL_ERROR
      "UbDiag ${MOONCAKE_UBDIAG_GIT_TAG} is missing include/ubdiag/perf_point.h")
  endif()
  file(STRINGS "${_MOONCAKE_UBDIAG_PERF_POINT_HEADER}"
       _MOONCAKE_UBDIAG_DISABLE_LINES REGEX "UBDIAG_DISABLE")
  if(NOT _MOONCAKE_UBDIAG_DISABLE_LINES)
    message(FATAL_ERROR
      "UbDiag ${MOONCAKE_UBDIAG_GIT_TAG} does not support UBDIAG_DISABLE. "
      "Select an UbDiag release that contains the compile-time disabled PerfPoint implementation.")
  endif()

  # Consumers inherit both the source include path and the compile-time switch.
  add_library(ubdiag_mock INTERFACE)
  target_include_directories(ubdiag_mock INTERFACE ${ubdiag_SOURCE_DIR}/include)
  target_compile_definitions(ubdiag_mock INTERFACE UBDIAG_DISABLE)
  add_library(UbDiag::ubdiag_lib ALIAS ubdiag_mock)

  set(MOONCAKE_UBDIAG_ACTIVE_LAYER "mock" CACHE STRING "" FORCE)
  message(STATUS "UbDiag: UBDIAG_DISABLE(空函数,零依赖)")
  return()
endif()

# ===== Layer 1: 编译真实 ubdiag =====
if(MOONCAKE_UBDIAG_SOURCE_DIR AND EXISTS "${MOONCAKE_UBDIAG_SOURCE_DIR}/CMakeLists.txt")
  FetchContent_Declare(ubdiag SOURCE_DIR ${MOONCAKE_UBDIAG_SOURCE_DIR})
else()
  FetchContent_Declare(ubdiag
    GIT_REPOSITORY ${MOONCAKE_UBDIAG_GIT_REPOSITORY}
    GIT_TAG ${MOONCAKE_UBDIAG_GIT_TAG})
endif()

set(BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(UBDIAG_BUILD_SHARED ON CACHE BOOL "" FORCE)
set(ENABLE_PERCENTILE ON CACHE BOOL "" FORCE)
set(ENABLE_PERFLOG ON CACHE BOOL "" FORCE)
set(ENABLE_OB_MEMORY OFF CACHE BOOL "" FORCE)
set(ENABLE_OB_CACHE OFF CACHE BOOL "" FORCE)
set(ENABLE_MEMPOINT OFF CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(ubdiag)

if(TARGET ubdiag_lib)
  # UbDiag master still uses CMAKE_SOURCE_DIR internally. When consumed by
  # FetchContent that variable points at Mooncake, so add the real source paths
  # to every UbDiag target without modifying the mirrored upstream sources.
  foreach(_MOONCAKE_UBDIAG_LIB_TARGET
          ubdiag_logger ubdiag_lib ubdiag_runtime_lib ubdiag_manager_lib
          ubdiag_bpf_loader)
    if(TARGET ${_MOONCAKE_UBDIAG_LIB_TARGET})
      target_include_directories(${_MOONCAKE_UBDIAG_LIB_TARGET} PUBLIC
        $<BUILD_INTERFACE:${ubdiag_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${ubdiag_SOURCE_DIR}/src>)
    endif()
  endforeach()
  if(TARGET ubdiag)
    target_include_directories(ubdiag PRIVATE
      ${ubdiag_SOURCE_DIR}/include
      ${ubdiag_SOURCE_DIR}/src
      ${ubdiag_SOURCE_DIR}/src/cli)
  endif()

  add_library(UbDiag::ubdiag_lib ALIAS ubdiag_lib)
  set(MOONCAKE_UBDIAG_ACTIVE_LAYER "vendored" CACHE STRING "" FORCE)
  message(STATUS "UbDiag: FetchContent 编译 ${MOONCAKE_UBDIAG_GIT_TAG}(库+CLI)")
endif()
