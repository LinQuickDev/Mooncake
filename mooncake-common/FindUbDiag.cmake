# FindUbDiag.cmake - two-layer UbDiag integration for Mooncake
#
# Layer 0 (default): Fetch the pinned UbDiag source, expose its public headers,
# and propagate UBDIAG_DISABLE. PerfPoint calls compile to constexpr no-op
# functions.
#
# Layer 1: Consume a separately installed system UbDiag package. The package
# must provide a shared SDK, the ubdiag CLI, percentile support, and PerfLog.
#
# Usage: include(${CMAKE_SOURCE_DIR}/mooncake-common/FindUbDiag.cmake)
# target_link_libraries(your_target PRIVATE UbDiag::ubdiag_lib)
#
# Options: -DMOONCAKE_ENABLE_UBDIAG=OFF  Layer 0 (default)
# -DMOONCAKE_ENABLE_UBDIAG=ON   Layer 1
#
# Layer 0 source selection: -DMOONCAKE_UBDIAG_GIT_TAG=<tag/branch/commit>
# -DMOONCAKE_UBDIAG_EXPECTED_COMMIT=<40-character SHA>
# -DMOONCAKE_UBDIAG_SOURCE_DIR=/path/to/ubdiag
#
# Layer 1 package selection: -DCMAKE_PREFIX_PATH=/path/to/ubdiag/prefix
# -DUbDiag_DIR=/path/to/lib64/cmake/UbDiag

if(TARGET UbDiag::ubdiag_lib)
  get_property(_MOONCAKE_UBDIAG_TARGET_OWNER GLOBAL
               PROPERTY MOONCAKE_UBDIAG_TARGET_OWNER)
  if(NOT _MOONCAKE_UBDIAG_TARGET_OWNER STREQUAL "${CMAKE_BINARY_DIR}")
    message(
      FATAL_ERROR
        "UbDiag::ubdiag_lib already exists but was not resolved by Mooncake. "
        "Remove the injected target and let FindUbDiag.cmake select the active "
        "Mooncake UbDiag layer.")
  endif()
  return()
endif()

option(MOONCAKE_ENABLE_UBDIAG
       "Use a system-installed UbDiag shared library and CLI" OFF)

function(_mooncake_write_ubdiag_manifest layer)
  set(_manifest "${CMAKE_BINARY_DIR}/mooncake_ubdiag.env")
  file(WRITE "${_manifest}" "MOONCAKE_UBDIAG_LAYER=${layer}\n")
  file(
    APPEND "${_manifest}"
    "MOONCAKE_UBDIAG_GIT_REPOSITORY=${MOONCAKE_UBDIAG_GIT_REPOSITORY}\n"
    "MOONCAKE_UBDIAG_GIT_TAG=${MOONCAKE_UBDIAG_GIT_TAG}\n"
    "MOONCAKE_UBDIAG_EXPECTED_COMMIT=${MOONCAKE_UBDIAG_EXPECTED_COMMIT}\n"
    "MOONCAKE_UBDIAG_RESOLVED_COMMIT=${MOONCAKE_UBDIAG_RESOLVED_COMMIT}\n"
    "MOONCAKE_UBDIAG_SOURCE_DIR=${MOONCAKE_UBDIAG_RESOLVED_SOURCE_DIR}\n"
    "MOONCAKE_UBDIAG_PACKAGE_VERSION=${UbDiag_VERSION}\n"
    "MOONCAKE_UBDIAG_PACKAGE_DIR=${UbDiag_DIR}\n"
    "MOONCAKE_UBDIAG_SYSTEM_PREFIX=${MOONCAKE_UBDIAG_SYSTEM_PREFIX}\n"
    "MOONCAKE_UBDIAG_SYSTEM_LIBRARY=${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}\n"
    "MOONCAKE_UBDIAG_SYSTEM_LIBRARY_RPM=${MOONCAKE_UBDIAG_SYSTEM_LIBRARY_RPM}\n"
    "MOONCAKE_UBDIAG_SYSTEM_CLI=${MOONCAKE_UBDIAG_SYSTEM_CLI}\n"
    "MOONCAKE_UBDIAG_SYSTEM_CLI_RPM=${MOONCAKE_UBDIAG_SYSTEM_CLI_RPM}\n"
    "MOONCAKE_UBDIAG_SYSTEM_RPM_EVR_ARCH=${MOONCAKE_UBDIAG_SYSTEM_RPM_EVR_ARCH}\n"
    "MOONCAKE_UBDIAG_SYSTEM_CONFIG=${MOONCAKE_UBDIAG_SYSTEM_CONFIG}\n")
  set(MOONCAKE_UBDIAG_MANIFEST
      "${_manifest}"
      CACHE FILEPATH "Resolved Mooncake UbDiag integration manifest" FORCE)
  set(MOONCAKE_UBDIAG_ACTIVE_LAYER
      "${layer}"
      CACHE STRING "Active UbDiag integration layer: mock or system" FORCE)
endfunction()

if(NOT MOONCAKE_ENABLE_UBDIAG)
  set(MOONCAKE_UBDIAG_GIT_REPOSITORY
      "https://github.com/LinQuickDev/ubdiag.git"
      CACHE STRING "UbDiag Git repository used by Layer 0")
  set(MOONCAKE_UBDIAG_GIT_TAG
      "8df2c2844d402e2e4dcd5ceab2424e8d36c5f99f"
      CACHE STRING "UbDiag ref used by Layer 0")
  set(MOONCAKE_UBDIAG_EXPECTED_COMMIT
      "8df2c2844d402e2e4dcd5ceab2424e8d36c5f99f"
      CACHE STRING "Allowed full UbDiag commit SHA used by Layer 0")
  set(MOONCAKE_UBDIAG_SOURCE_DIR
      ""
      CACHE PATH "Local UbDiag source for offline Layer 0 configuration")

  include(FetchContent)

  function(_mooncake_verify_ubdiag_source source_dir)
    get_filename_component(_source_dir "${source_dir}" REALPATH)
    string(TOLOWER "${MOONCAKE_UBDIAG_EXPECTED_COMMIT}" _expected_commit)
    string(LENGTH "${_expected_commit}" _expected_commit_length)
    if(NOT _expected_commit_length EQUAL 40 OR NOT _expected_commit MATCHES
                                               "^[0-9a-f]+$")
      message(
        FATAL_ERROR
          "MOONCAKE_UBDIAG_EXPECTED_COMMIT must be a full 40-character Git "
          "SHA, got: ${MOONCAKE_UBDIAG_EXPECTED_COMMIT}")
    endif()

    find_program(_MOONCAKE_UBDIAG_GIT_EXECUTABLE NAMES git)
    if(NOT _MOONCAKE_UBDIAG_GIT_EXECUTABLE OR NOT EXISTS
                                              "${_source_dir}/CMakeLists.txt")
      message(
        FATAL_ERROR
          "UbDiag source is incomplete or git is unavailable: ${_source_dir}")
    endif()

    execute_process(
      COMMAND "${_MOONCAKE_UBDIAG_GIT_EXECUTABLE}" -C "${_source_dir}" rev-parse
              --verify HEAD
      RESULT_VARIABLE _git_result
      OUTPUT_VARIABLE _resolved_commit
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    string(TOLOWER "${_resolved_commit}" _resolved_commit)
    if(NOT _git_result EQUAL 0 OR NOT _resolved_commit STREQUAL
                                  _expected_commit)
      message(
        FATAL_ERROR
          "UbDiag source commit mismatch.\n"
          "  expected: ${_expected_commit}\n"
          "  resolved: ${_resolved_commit}\n"
          "  source:   ${_source_dir}\n"
          "Remove the stale _deps/ubdiag-src directory or select the matching "
          "MOONCAKE_UBDIAG_EXPECTED_COMMIT.")
    endif()

    execute_process(
      COMMAND "${_MOONCAKE_UBDIAG_GIT_EXECUTABLE}" -C "${_source_dir}" status
              --porcelain --untracked-files=all
      RESULT_VARIABLE _git_result
      OUTPUT_VARIABLE _git_status
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT _git_result EQUAL 0 OR NOT _git_status STREQUAL "")
      message(
        FATAL_ERROR
          "UbDiag source worktree is not clean, so its HEAD SHA does not fully "
          "describe the headers that would be used:\n${_git_status}")
    endif()

    set(MOONCAKE_UBDIAG_RESOLVED_SOURCE_DIR
        "${_source_dir}"
        CACHE PATH "Verified UbDiag source selected by Mooncake" FORCE)
    set(MOONCAKE_UBDIAG_RESOLVED_COMMIT
        "${_resolved_commit}"
        CACHE STRING "Verified UbDiag source commit selected by Mooncake" FORCE)
    set(MOONCAKE_UBDIAG_RESOLVED_SOURCE_DIR
        "${_source_dir}"
        PARENT_SCOPE)
    set(MOONCAKE_UBDIAG_RESOLVED_COMMIT
        "${_resolved_commit}"
        PARENT_SCOPE)
  endfunction()

  if(MOONCAKE_UBDIAG_SOURCE_DIR)
    if(NOT EXISTS "${MOONCAKE_UBDIAG_SOURCE_DIR}/CMakeLists.txt")
      message(
        FATAL_ERROR
          "MOONCAKE_UBDIAG_SOURCE_DIR does not contain CMakeLists.txt: "
          "${MOONCAKE_UBDIAG_SOURCE_DIR}")
    endif()
    set(ubdiag_SOURCE_DIR "${MOONCAKE_UBDIAG_SOURCE_DIR}")
  else()
    FetchContent_Populate(
      ubdiag
      GIT_REPOSITORY "${MOONCAKE_UBDIAG_GIT_REPOSITORY}"
      GIT_TAG "${MOONCAKE_UBDIAG_GIT_TAG}"
      SOURCE_DIR "${FETCHCONTENT_BASE_DIR}/ubdiag-src"
      SUBBUILD_DIR "${FETCHCONTENT_BASE_DIR}/ubdiag-subbuild")
  endif()
  _mooncake_verify_ubdiag_source("${ubdiag_SOURCE_DIR}")

  set(_MOONCAKE_UBDIAG_PERF_POINT_HEADER
      "${ubdiag_SOURCE_DIR}/include/ubdiag/perf_point.h")
  if(NOT EXISTS "${_MOONCAKE_UBDIAG_PERF_POINT_HEADER}")
    message(FATAL_ERROR "UbDiag ${MOONCAKE_UBDIAG_GIT_TAG} is missing "
                        "include/ubdiag/perf_point.h")
  endif()
  file(STRINGS "${_MOONCAKE_UBDIAG_PERF_POINT_HEADER}"
       _MOONCAKE_UBDIAG_DISABLE_LINES REGEX "UBDIAG_DISABLE")
  if(NOT _MOONCAKE_UBDIAG_DISABLE_LINES)
    message(
      FATAL_ERROR "UbDiag ${MOONCAKE_UBDIAG_GIT_TAG} does not provide the "
                  "UBDIAG_DISABLE PerfPoint implementation required by Layer 0."
    )
  endif()

  add_library(ubdiag_mock INTERFACE)
  target_include_directories(ubdiag_mock
                             INTERFACE "${ubdiag_SOURCE_DIR}/include")
  target_compile_definitions(ubdiag_mock INTERFACE UBDIAG_DISABLE)
  add_library(UbDiag::ubdiag_lib ALIAS ubdiag_mock)

  set(MOONCAKE_UBDIAG_SYSTEM_PREFIX "")
  set(MOONCAKE_UBDIAG_SYSTEM_LIBRARY "")
  set(MOONCAKE_UBDIAG_SYSTEM_LIBRARY_RPM "")
  set(MOONCAKE_UBDIAG_SYSTEM_CLI "")
  set(MOONCAKE_UBDIAG_SYSTEM_CLI_RPM "")
  set(MOONCAKE_UBDIAG_SYSTEM_RPM_EVR_ARCH "")
  set(MOONCAKE_UBDIAG_SYSTEM_CONFIG "")
  set(MOONCAKE_UBDIAG_LIBRARY_DIR "")
  set(UbDiag_VERSION "")
  set(UbDiag_DIR "")
  set_property(GLOBAL PROPERTY MOONCAKE_UBDIAG_TARGET_OWNER
                               "${CMAKE_BINARY_DIR}")
  _mooncake_write_ubdiag_manifest("mock")
  message(STATUS "UbDiag: Layer 0 UBDIAG_DISABLE headers, verified "
                 "${MOONCAKE_UBDIAG_RESOLVED_COMMIT}")
  return()
endif()

# Layer 1 must be fully supplied by one system UbDiag RPM installation.
find_package(UbDiag CONFIG QUIET)
if(NOT UbDiag_FOUND)
  message(
    FATAL_ERROR
      "MOONCAKE_ENABLE_UBDIAG=ON requires a system-installed UbDiag "
      "shared SDK and CLI.\n"
      "Install the UbDiag RPM separately (or build an UbDiag RPM with "
      "UBDIAG_BUILD_SHARED=ON, ENABLE_PERCENTILE=ON, and ENABLE_PERFLOG=ON), "
      "then install that RPM before configuring Mooncake.\n"
      "Mooncake does not download or build the real UbDiag runtime in Layer 1. "
      "Use CMAKE_PREFIX_PATH or UbDiag_DIR when it is installed under a "
      "non-standard system prefix.")
endif()
if(NOT TARGET UbDiag::ubdiag_lib)
  message(
    FATAL_ERROR
      "The system UbDiag package was found at ${UbDiag_DIR}, but it does not "
      "export the required UbDiag::ubdiag_lib target. Reinstall the complete "
      "UbDiag development RPM.")
endif()

get_target_property(_MOONCAKE_UBDIAG_IMPORTED UbDiag::ubdiag_lib IMPORTED)
get_target_property(_MOONCAKE_UBDIAG_TARGET_TYPE UbDiag::ubdiag_lib TYPE)
if(NOT _MOONCAKE_UBDIAG_IMPORTED OR NOT _MOONCAKE_UBDIAG_TARGET_TYPE STREQUAL
                                    "SHARED_LIBRARY")
  message(
    FATAL_ERROR
      "MOONCAKE_ENABLE_UBDIAG=ON requires the system shared SDK "
      "libubdiag.so; ${UbDiag_DIR} exported target type "
      "'${_MOONCAKE_UBDIAG_TARGET_TYPE}'. Rebuild and reinstall the UbDiag RPM "
      "with UBDIAG_BUILD_SHARED=ON.")
endif()

function(_mooncake_get_imported_location target output_variable)
  get_target_property(_imported_configs "${target}" IMPORTED_CONFIGURATIONS)
  foreach(_config IN LISTS _imported_configs)
    string(TOUPPER "${_config}" _config_upper)
    get_target_property(_candidate "${target}"
                        "IMPORTED_LOCATION_${_config_upper}")
    if(_candidate AND NOT _candidate MATCHES "-NOTFOUND$")
      set("${output_variable}"
          "${_candidate}"
          PARENT_SCOPE)
      return()
    endif()
  endforeach()
  get_target_property(_candidate "${target}" IMPORTED_LOCATION)
  if(_candidate AND NOT _candidate MATCHES "-NOTFOUND$")
    set("${output_variable}"
        "${_candidate}"
        PARENT_SCOPE)
  endif()
endfunction()

_mooncake_get_imported_location(UbDiag::ubdiag_lib
                                _MOONCAKE_UBDIAG_IMPORTED_LOCATION)
if(NOT _MOONCAKE_UBDIAG_IMPORTED_LOCATION
   OR NOT EXISTS "${_MOONCAKE_UBDIAG_IMPORTED_LOCATION}")
  message(
    FATAL_ERROR
      "The system UbDiag target does not resolve to an installed shared "
      "library: ${_MOONCAKE_UBDIAG_IMPORTED_LOCATION}")
endif()
get_filename_component(MOONCAKE_UBDIAG_SYSTEM_LIBRARY
                       "${_MOONCAKE_UBDIAG_IMPORTED_LOCATION}" REALPATH)
get_filename_component(MOONCAKE_UBDIAG_LIBRARY_DIR
                       "${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}" DIRECTORY)
get_filename_component(_MOONCAKE_UBDIAG_LIBRARY_NAME
                       "${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}" NAME)
if(NOT _MOONCAKE_UBDIAG_LIBRARY_NAME MATCHES "^libubdiag\\.so(\\..*)?$")
  message(
    FATAL_ERROR "The system UbDiag target resolved to an unexpected library: "
                "${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}")
endif()

get_target_property(_MOONCAKE_UBDIAG_COMPILE_DEFINITIONS UbDiag::ubdiag_lib
                    INTERFACE_COMPILE_DEFINITIONS)
foreach(_required_definition UBDIAG_ENABLE_PERCENTILE UBDIAG_ENABLE_PERFLOG)
  list(FIND _MOONCAKE_UBDIAG_COMPILE_DEFINITIONS "${_required_definition}"
       _definition_index)
  if(_definition_index EQUAL -1)
    message(
      FATAL_ERROR
        "The system UbDiag package does not advertise "
        "${_required_definition}. Rebuild and reinstall the UbDiag RPM with "
        "ENABLE_PERCENTILE=ON and ENABLE_PERFLOG=ON.")
  endif()
endforeach()

get_filename_component(_MOONCAKE_UBDIAG_LIBDIR_NAME
                       "${MOONCAKE_UBDIAG_LIBRARY_DIR}" NAME)
if(_MOONCAKE_UBDIAG_LIBDIR_NAME MATCHES "^lib(64)?$")
  get_filename_component(MOONCAKE_UBDIAG_SYSTEM_PREFIX
                         "${MOONCAKE_UBDIAG_LIBRARY_DIR}" DIRECTORY)
else()
  get_filename_component(_MOONCAKE_UBDIAG_CMAKE_DIR_PARENT "${UbDiag_DIR}"
                         DIRECTORY)
  get_filename_component(_MOONCAKE_UBDIAG_PACKAGE_LIBDIR
                         "${_MOONCAKE_UBDIAG_CMAKE_DIR_PARENT}" DIRECTORY)
  get_filename_component(MOONCAKE_UBDIAG_SYSTEM_PREFIX
                         "${_MOONCAKE_UBDIAG_PACKAGE_LIBDIR}" DIRECTORY)
endif()

unset(_MOONCAKE_UBDIAG_CLI CACHE)
find_program(
  _MOONCAKE_UBDIAG_CLI
  NAMES ubdiag
  PATHS "${MOONCAKE_UBDIAG_SYSTEM_PREFIX}/bin"
  NO_DEFAULT_PATH)
if(NOT _MOONCAKE_UBDIAG_CLI)
  message(
    FATAL_ERROR
      "The system UbDiag shared SDK was found at "
      "${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}, but the matching ubdiag CLI was not "
      "found at ${MOONCAKE_UBDIAG_SYSTEM_PREFIX}/bin/ubdiag. Install the "
      "complete UbDiag RPM before configuring Mooncake.")
endif()
get_filename_component(MOONCAKE_UBDIAG_SYSTEM_CLI "${_MOONCAKE_UBDIAG_CLI}"
                       REALPATH)

find_program(_MOONCAKE_UBDIAG_RPM_EXECUTABLE NAMES rpm)
if(NOT _MOONCAKE_UBDIAG_RPM_EXECUTABLE)
  message(
    FATAL_ERROR
      "MOONCAKE_ENABLE_UBDIAG=ON requires the rpm command to verify that the "
      "system libubdiag.so and ubdiag CLI belong to matching UbDiag RPMs.")
endif()

function(_mooncake_query_ubdiag_rpm file_path owner_output identity_output)
  execute_process(
    COMMAND "${_MOONCAKE_UBDIAG_RPM_EXECUTABLE}" -qf --qf
            "%{NAME}|%{VERSION}-%{RELEASE}.%{ARCH}" "${file_path}"
    RESULT_VARIABLE _rpm_result
    OUTPUT_VARIABLE _rpm_record
    ERROR_VARIABLE _rpm_error
    OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_STRIP_TRAILING_WHITESPACE)
  if(NOT _rpm_result EQUAL 0 OR NOT _rpm_record MATCHES "^([^|]+)\\|(.+)$")
    message(
      FATAL_ERROR
        "${file_path} is not owned by an installed RPM package. "
        "Install the complete UbDiag RPM before configuring Mooncake.\n"
        "rpm -qf: ${_rpm_error}")
  endif()
  set(_rpm_owner "${CMAKE_MATCH_1}")
  set(_rpm_identity "${CMAKE_MATCH_2}")
  string(TOLOWER "${_rpm_owner}" _rpm_owner_lower)
  if(NOT _rpm_owner_lower MATCHES "ubdiag")
    message(
      FATAL_ERROR
        "${file_path} is owned by '${_rpm_owner}', not an UbDiag RPM package.")
  endif()
  set("${owner_output}"
      "${_rpm_owner}"
      PARENT_SCOPE)
  set("${identity_output}"
      "${_rpm_identity}"
      PARENT_SCOPE)
endfunction()

_mooncake_query_ubdiag_rpm(
  "${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}" MOONCAKE_UBDIAG_SYSTEM_LIBRARY_RPM
  _MOONCAKE_UBDIAG_LIBRARY_RPM_IDENTITY)
_mooncake_query_ubdiag_rpm(
  "${MOONCAKE_UBDIAG_SYSTEM_CLI}" MOONCAKE_UBDIAG_SYSTEM_CLI_RPM
  _MOONCAKE_UBDIAG_CLI_RPM_IDENTITY)
if(NOT _MOONCAKE_UBDIAG_LIBRARY_RPM_IDENTITY STREQUAL
   _MOONCAKE_UBDIAG_CLI_RPM_IDENTITY)
  message(
    FATAL_ERROR
      "The system libubdiag.so and ubdiag CLI come from different RPM "
      "versions.\n"
      "  library: ${MOONCAKE_UBDIAG_SYSTEM_LIBRARY_RPM} "
      "${_MOONCAKE_UBDIAG_LIBRARY_RPM_IDENTITY}\n"
      "  CLI:     ${MOONCAKE_UBDIAG_SYSTEM_CLI_RPM} "
      "${_MOONCAKE_UBDIAG_CLI_RPM_IDENTITY}\n"
      "Reinstall one complete, matching UbDiag RPM set.")
endif()
set(MOONCAKE_UBDIAG_SYSTEM_RPM_EVR_ARCH
    "${_MOONCAKE_UBDIAG_LIBRARY_RPM_IDENTITY}")

unset(_MOONCAKE_UBDIAG_CONFIG CACHE)
set(_MOONCAKE_UBDIAG_CONFIG_PATHS "${MOONCAKE_UBDIAG_SYSTEM_PREFIX}/etc/ubdiag")
if(MOONCAKE_UBDIAG_SYSTEM_PREFIX STREQUAL "/usr")
  list(APPEND _MOONCAKE_UBDIAG_CONFIG_PATHS "/etc/ubdiag")
endif()
find_file(
  _MOONCAKE_UBDIAG_CONFIG
  NAMES ubdiag.conf
  PATHS ${_MOONCAKE_UBDIAG_CONFIG_PATHS}
  NO_DEFAULT_PATH)
if(_MOONCAKE_UBDIAG_CONFIG)
  get_filename_component(MOONCAKE_UBDIAG_SYSTEM_CONFIG
                         "${_MOONCAKE_UBDIAG_CONFIG}" REALPATH)
else()
  set(MOONCAKE_UBDIAG_SYSTEM_CONFIG "")
endif()

set(MOONCAKE_UBDIAG_SYSTEM_PREFIX
    "${MOONCAKE_UBDIAG_SYSTEM_PREFIX}"
    CACHE PATH "System UbDiag installation prefix selected by Mooncake" FORCE)
set(MOONCAKE_UBDIAG_SYSTEM_LIBRARY
    "${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}"
    CACHE FILEPATH "System libubdiag.so selected by Mooncake" FORCE)
set(MOONCAKE_UBDIAG_SYSTEM_LIBRARY_RPM
    "${MOONCAKE_UBDIAG_SYSTEM_LIBRARY_RPM}"
    CACHE STRING
          "RPM package owning the system libubdiag.so selected by Mooncake"
          FORCE)
set(MOONCAKE_UBDIAG_SYSTEM_CLI
    "${MOONCAKE_UBDIAG_SYSTEM_CLI}"
    CACHE FILEPATH "System ubdiag CLI selected by Mooncake" FORCE)
set(MOONCAKE_UBDIAG_SYSTEM_CLI_RPM
    "${MOONCAKE_UBDIAG_SYSTEM_CLI_RPM}"
    CACHE STRING
          "RPM package owning the system ubdiag CLI selected by Mooncake" FORCE)
set(MOONCAKE_UBDIAG_SYSTEM_RPM_EVR_ARCH
    "${MOONCAKE_UBDIAG_SYSTEM_RPM_EVR_ARCH}"
    CACHE STRING
          "Matching system UbDiag RPM version-release.arch selected by Mooncake"
          FORCE)
set(MOONCAKE_UBDIAG_SYSTEM_CONFIG
    "${MOONCAKE_UBDIAG_SYSTEM_CONFIG}"
    CACHE FILEPATH "System UbDiag configuration selected by Mooncake" FORCE)
set(MOONCAKE_UBDIAG_LIBRARY_DIR
    "${MOONCAKE_UBDIAG_LIBRARY_DIR}"
    CACHE PATH "System UbDiag library directory selected by Mooncake" FORCE)
set(MOONCAKE_UBDIAG_GIT_REPOSITORY "")
set(MOONCAKE_UBDIAG_GIT_TAG "")
set(MOONCAKE_UBDIAG_EXPECTED_COMMIT "")
set(MOONCAKE_UBDIAG_RESOLVED_COMMIT "")
set(MOONCAKE_UBDIAG_RESOLVED_SOURCE_DIR "")
set_property(GLOBAL PROPERTY MOONCAKE_UBDIAG_TARGET_OWNER "${CMAKE_BINARY_DIR}")
_mooncake_write_ubdiag_manifest("system")
message(
  STATUS "UbDiag: Layer 1 system RPM ${MOONCAKE_UBDIAG_SYSTEM_RPM_EVR_ARCH}; "
         "library=${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}; "
         "CLI=${MOONCAKE_UBDIAG_SYSTEM_CLI}")
