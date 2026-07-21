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
#   -DMOONCAKE_UBDIAG_GIT_TAG=v0.5.0  指定 ubdiag 版本
#   -DMOONCAKE_UBDIAG_SOURCE_DIR=/path/to/ubdiag  离线模式指定本地源码

if(TARGET UbDiag::ubdiag_lib)
  return()
endif()

option(MOONCAKE_ENABLE_UBDIAG "编译 ubdiag 真实库(否则用 UBDIAG_DISABLE 空函数)" OFF)
set(MOONCAKE_UBDIAG_GIT_TAG "v0.5.1" CACHE STRING "ubdiag 版本(tag/branch/commit)")
set(MOONCAKE_UBDIAG_SOURCE_DIR "" CACHE PATH "本地 ubdiag 源码目录(离线用,为空则 FetchContent)")

include(FetchContent)

# ===== 始终 FetchContent 拉取 ubdiag 源码(头文件必备) =====
if(MOONCAKE_UBDIAG_SOURCE_DIR AND EXISTS "${MOONCAKE_UBDIAG_SOURCE_DIR}/CMakeLists.txt")
  FetchContent_Declare(ubdiag SOURCE_DIR ${MOONCAKE_UBDIAG_SOURCE_DIR})
else()
  FetchContent_Declare(ubdiag
      GIT_REPOSITORY https://atomgit.com/liusiyu60/ubdiag.git
      GIT_TAG ${MOONCAKE_UBDIAG_GIT_TAG})
endif()

# ===== Layer 0: UBDIAG_DISABLE 模式(默认) =====
if(NOT MOONCAKE_ENABLE_UBDIAG)
  FetchContent_MakeAvailable(ubdiag)

  # PerfPoint 变成 constexpr 空函数,编译器完全优化掉
  add_compile_definitions(UBDIAG_DISABLE)

  # 提供头文件路径
  add_library(ubdiag_mock INTERFACE)
  target_include_directories(ubdiag_mock INTERFACE ${ubdiag_SOURCE_DIR}/include)
  add_library(UbDiag::ubdiag_lib ALIAS ubdiag_mock)

  set(MOONCAKE_UBDIAG_ACTIVE_LAYER "mock" CACHE STRING "" FORCE)
  message(STATUS "UbDiag: UBDIAG_DISABLE(空函数,零依赖)")
  return()
endif()

# ===== Layer 1: 编译真实 ubdiag =====
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
  add_library(UbDiag::ubdiag_lib ALIAS ubdiag_lib)
  set(MOONCAKE_UBDIAG_ACTIVE_LAYER "vendored" CACHE STRING "" FORCE)
  message(STATUS "UbDiag: FetchContent 编译 ${MOONCAKE_UBDIAG_GIT_TAG}(库+CLI)")
endif()
