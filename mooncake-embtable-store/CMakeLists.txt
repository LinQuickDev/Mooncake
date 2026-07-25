cmake_minimum_required(VERSION 3.16)
project(mooncake_embtable_store VERSION 0.1.0 LANGUAGES CXX)

find_package(UbDiag REQUIRED)

# This module builds on top of the parent Mooncake project's mooncake_store
# target (design doc section 8.9). It is added as a subdirectory of the
# top-level Mooncake CMakeLists.txt, so global options (C++20, glog, yalantinglibs,
# xxhash, etc.) are already configured there.

# ---- Third-party: boomphf (BBHash) for perfect hashing (design doc 8.3) ----
include(FetchContent)
# Disconnect from network after initial fetch so builds work offline.
set(FETCHCONTENT_UPDATES_DISCONNECTED ON CACHE BOOL "" FORCE)
FetchContent_Declare(
    boomphf
    GIT_REPOSITORY https://github.com/rizkg/BBHash.git
    GIT_TAG        master
)
# BBHash is header-only; suppress its sub-build.
FetchContent_MakeAvailable(boomphf)
# boomphf_SOURCE_DIR now points to the fetched headers (BooPHF.h).

# ---- Include directories ----
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${boomphf_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-store/include
    ${CMAKE_CURRENT_BINARY_DIR}/../mooncake-store/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-transfer-engine/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-common/include
)

# The library is built up layer by layer. Each layer adds its own sources and
# depends on the layer below it (design doc section 8.11).
add_subdirectory(src)

# Tests are added once the full stack builds (design doc section 8.11).
