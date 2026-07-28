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
#   -DMOONCAKE_UBDIAG_EXPECTED_COMMIT=<40-char SHA>  指定允许的精确提交
#   -DMOONCAKE_UBDIAG_SOURCE_DIR=/path/to/ubdiag  离线模式指定本地源码

if(TARGET UbDiag::ubdiag_lib)
  get_property(_MOONCAKE_UBDIAG_TARGET_OWNER GLOBAL
               PROPERTY MOONCAKE_UBDIAG_TARGET_OWNER)
  if(NOT _MOONCAKE_UBDIAG_TARGET_OWNER STREQUAL "${CMAKE_BINARY_DIR}")
    message(FATAL_ERROR
      "UbDiag::ubdiag_lib already exists but was not created by Mooncake's "
      "FetchContent integration. Remove any find_package(UbDiag), system "
      "UbDiag target, or injected parent-project target before configuring.")
  endif()
  return()
endif()

option(MOONCAKE_ENABLE_UBDIAG "编译 ubdiag 真实库(否则用 UBDIAG_DISABLE 空函数)" OFF)
set(MOONCAKE_UBDIAG_GIT_REPOSITORY
    "https://github.com/LinQuickDev/ubdiag.git"
    CACHE STRING "ubdiag Git repository")
set(MOONCAKE_UBDIAG_GIT_TAG
    "8df2c2844d402e2e4dcd5ceab2424e8d36c5f99f"
    CACHE STRING "ubdiag 版本(tag/branch/commit)")
set(MOONCAKE_UBDIAG_EXPECTED_COMMIT
    "8df2c2844d402e2e4dcd5ceab2424e8d36c5f99f"
    CACHE STRING "Mooncake 允许使用的 ubdiag 精确提交 SHA")
set(MOONCAKE_UBDIAG_SOURCE_DIR "" CACHE PATH "本地 ubdiag 源码目录(离线用,为空则 FetchContent)")

include(FetchContent)

function(_mooncake_verify_ubdiag_source source_dir)
  get_filename_component(_source_dir "${source_dir}" REALPATH)
  string(TOLOWER "${MOONCAKE_UBDIAG_EXPECTED_COMMIT}" _expected_commit)
  string(LENGTH "${_expected_commit}" _expected_commit_length)
  if(NOT _expected_commit_length EQUAL 40 OR
     NOT _expected_commit MATCHES "^[0-9a-f]+$")
    message(FATAL_ERROR
      "MOONCAKE_UBDIAG_EXPECTED_COMMIT must be a full 40-character Git SHA, "
      "got: ${MOONCAKE_UBDIAG_EXPECTED_COMMIT}")
  endif()

  find_program(_MOONCAKE_UBDIAG_GIT_EXECUTABLE NAMES git)
  if(NOT _MOONCAKE_UBDIAG_GIT_EXECUTABLE OR
     NOT EXISTS "${_source_dir}/CMakeLists.txt")
    message(FATAL_ERROR
      "UbDiag source is incomplete or git is unavailable: ${_source_dir}")
  endif()

  execute_process(
    COMMAND "${_MOONCAKE_UBDIAG_GIT_EXECUTABLE}" -C "${_source_dir}"
            rev-parse --verify HEAD
    RESULT_VARIABLE _git_result
    OUTPUT_VARIABLE _resolved_commit
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  string(TOLOWER "${_resolved_commit}" _resolved_commit)
  if(NOT _git_result EQUAL 0 OR
     NOT _resolved_commit STREQUAL _expected_commit)
    message(FATAL_ERROR
      "UbDiag source commit mismatch.\n"
      "  expected: ${_expected_commit}\n"
      "  resolved: ${_resolved_commit}\n"
      "  source:   ${_source_dir}\n"
      "Remove the stale _deps/ubdiag-src directory or select the matching "
      "MOONCAKE_UBDIAG_EXPECTED_COMMIT.")
  endif()

  execute_process(
    COMMAND "${_MOONCAKE_UBDIAG_GIT_EXECUTABLE}" -C "${_source_dir}"
            status --porcelain --untracked-files=all
    RESULT_VARIABLE _git_result
    OUTPUT_VARIABLE _git_status
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(NOT _git_result EQUAL 0 OR NOT _git_status STREQUAL "")
    message(FATAL_ERROR
      "UbDiag source worktree is not clean, so its HEAD SHA does not fully "
      "describe the code that would be built:\n${_git_status}")
  endif()

  set(MOONCAKE_UBDIAG_RESOLVED_SOURCE_DIR "${_source_dir}"
      CACHE PATH "Verified UbDiag source selected by Mooncake" FORCE)
  set(MOONCAKE_UBDIAG_RESOLVED_COMMIT "${_resolved_commit}"
      CACHE STRING "Verified UbDiag source commit selected by Mooncake" FORCE)
  set(MOONCAKE_UBDIAG_RESOLVED_SOURCE_DIR "${_source_dir}" PARENT_SCOPE)
  set(MOONCAKE_UBDIAG_RESOLVED_COMMIT "${_resolved_commit}" PARENT_SCOPE)
endfunction()

function(_mooncake_write_ubdiag_rpm_manifest layer)
  set(_manifest "${CMAKE_BINARY_DIR}/mooncake_ubdiag_rpm.env")
  file(WRITE "${_manifest}" "MOONCAKE_UBDIAG_LAYER=${layer}\n")
  file(APPEND "${_manifest}"
    "MOONCAKE_UBDIAG_GIT_REPOSITORY=${MOONCAKE_UBDIAG_GIT_REPOSITORY}\n"
    "MOONCAKE_UBDIAG_GIT_TAG=${MOONCAKE_UBDIAG_GIT_TAG}\n"
    "MOONCAKE_UBDIAG_EXPECTED_COMMIT=${MOONCAKE_UBDIAG_EXPECTED_COMMIT}\n"
    "MOONCAKE_UBDIAG_RESOLVED_COMMIT=${MOONCAKE_UBDIAG_RESOLVED_COMMIT}\n"
    "MOONCAKE_UBDIAG_SOURCE_DIR=${MOONCAKE_UBDIAG_RESOLVED_SOURCE_DIR}\n")
  set(MOONCAKE_UBDIAG_RPM_MANIFEST "${_manifest}"
      CACHE FILEPATH "Verified UbDiag RPM packaging manifest" FORCE)
  set(MOONCAKE_UBDIAG_ACTIVE_LAYER "${layer}"
      CACHE STRING "Active UbDiag integration layer: mock or vendored" FORCE)
endfunction()

set(ubdiag_BINARY_DIR "${FETCHCONTENT_BASE_DIR}/ubdiag-build")
if(MOONCAKE_UBDIAG_SOURCE_DIR)
  if(NOT EXISTS "${MOONCAKE_UBDIAG_SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR
      "MOONCAKE_UBDIAG_SOURCE_DIR does not contain CMakeLists.txt: "
      "${MOONCAKE_UBDIAG_SOURCE_DIR}")
  endif()
  set(ubdiag_SOURCE_DIR "${MOONCAKE_UBDIAG_SOURCE_DIR}")
else()
  FetchContent_Populate(ubdiag
    GIT_REPOSITORY ${MOONCAKE_UBDIAG_GIT_REPOSITORY}
    GIT_TAG ${MOONCAKE_UBDIAG_GIT_TAG}
    SOURCE_DIR "${FETCHCONTENT_BASE_DIR}/ubdiag-src"
    BINARY_DIR "${ubdiag_BINARY_DIR}"
    SUBBUILD_DIR "${FETCHCONTENT_BASE_DIR}/ubdiag-subbuild")
endif()
_mooncake_verify_ubdiag_source("${ubdiag_SOURCE_DIR}")

# ===== Layer 0: UBDIAG_DISABLE 模式(默认) =====
if(NOT MOONCAKE_ENABLE_UBDIAG)
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

  set_property(GLOBAL PROPERTY MOONCAKE_UBDIAG_TARGET_OWNER
               "${CMAKE_BINARY_DIR}")
  _mooncake_write_ubdiag_rpm_manifest("mock")
  message(STATUS
    "UbDiag: UBDIAG_DISABLE(空函数,零依赖), verified ${MOONCAKE_UBDIAG_RESOLVED_COMMIT}")
  return()
endif()

# ===== Layer 1: 编译真实 ubdiag =====
set(_MOONCAKE_UBDIAG_BUILD_COMMIT_FILE
    "${ubdiag_BINARY_DIR}/mooncake-source-commit.txt")
set(_MOONCAKE_UBDIAG_REUSE_BINARY_DIR TRUE)
if(EXISTS "${_MOONCAKE_UBDIAG_BUILD_COMMIT_FILE}")
  file(READ "${_MOONCAKE_UBDIAG_BUILD_COMMIT_FILE}"
       _MOONCAKE_UBDIAG_PREVIOUS_BUILD_COMMIT)
  string(STRIP "${_MOONCAKE_UBDIAG_PREVIOUS_BUILD_COMMIT}"
         _MOONCAKE_UBDIAG_PREVIOUS_BUILD_COMMIT)
  if(NOT _MOONCAKE_UBDIAG_PREVIOUS_BUILD_COMMIT STREQUAL
         MOONCAKE_UBDIAG_RESOLVED_COMMIT)
    set(_MOONCAKE_UBDIAG_REUSE_BINARY_DIR FALSE)
  endif()
elseif(EXISTS "${ubdiag_BINARY_DIR}")
  file(GLOB _MOONCAKE_UBDIAG_EXISTING_BUILD_FILES
       "${ubdiag_BINARY_DIR}/*")
  if(_MOONCAKE_UBDIAG_EXISTING_BUILD_FILES)
    set(_MOONCAKE_UBDIAG_REUSE_BINARY_DIR FALSE)
  endif()
endif()
if(NOT _MOONCAKE_UBDIAG_REUSE_BINARY_DIR)
  message(STATUS
    "UbDiag: removing binary directory from a different or unverified source")
  file(REMOVE_RECURSE "${ubdiag_BINARY_DIR}")
endif()
file(MAKE_DIRECTORY "${ubdiag_BINARY_DIR}")
file(WRITE "${_MOONCAKE_UBDIAG_BUILD_COMMIT_FILE}"
     "${MOONCAKE_UBDIAG_RESOLVED_COMMIT}\n")

set(UBDIAG_BUILD_SHARED ON CACHE BOOL "" FORCE)
set(ENABLE_PERCENTILE ON CACHE BOOL "" FORCE)
set(ENABLE_PERFLOG ON CACHE BOOL "" FORCE)
set(ENABLE_OB_MEMORY OFF CACHE BOOL "" FORCE)
set(ENABLE_OB_CACHE OFF CACHE BOOL "" FORCE)
set(ENABLE_MEMPOINT OFF CACHE BOOL "" FORCE)
set(UBDIAG_ENABLE_CACHEPOINT OFF CACHE BOOL "" FORCE)

function(_mooncake_make_ubdiag_available)
  # UbDiag still exposes generic BUILD_* options. Keep the overrides inside a
  # function scope so Mooncake's own BUILD_EXAMPLES value is not changed.
  set(BUILD_TESTS OFF)
  set(BUILD_EXAMPLES OFF)
  add_subdirectory("${ubdiag_SOURCE_DIR}" "${ubdiag_BINARY_DIR}")
endfunction()
_mooncake_make_ubdiag_available()

if(NOT TARGET ubdiag_lib OR NOT TARGET ubdiag)
  message(FATAL_ERROR
    "UbDiag ${MOONCAKE_UBDIAG_GIT_TAG} did not create both ubdiag_lib and CLI targets")
endif()

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
target_include_directories(ubdiag PRIVATE
  ${ubdiag_SOURCE_DIR}/include
  ${ubdiag_SOURCE_DIR}/src
  ${ubdiag_SOURCE_DIR}/src/cli)

add_library(UbDiag::ubdiag_lib ALIAS ubdiag_lib)
set_property(GLOBAL PROPERTY MOONCAKE_UBDIAG_TARGET_OWNER
             "${CMAKE_BINARY_DIR}")
_mooncake_write_ubdiag_rpm_manifest("vendored")
message(STATUS
  "UbDiag: FetchContent 编译 ${MOONCAKE_UBDIAG_GIT_TAG}(库+CLI), "
  "verified ${MOONCAKE_UBDIAG_RESOLVED_COMMIT}")
