# Two-layer SpDiag integration for Mooncake.
#
# Layer 0 (default) uses the SpDiag public headers with SPDIAG_DISABLE. Layer 1
# consumes a shared SpDiag SDK and CLI installed from system RPMs.

include_guard(GLOBAL)

option(MOONCAKE_ENABLE_SPDIAG
       "Use the system-installed SpDiag shared library and CLI" OFF)

function(_mooncake_write_spdiag_manifest layer library cli config)
  file(
    WRITE "${CMAKE_BINARY_DIR}/mooncake_spdiag.env"
    "MOONCAKE_SPDIAG_LAYER=${layer}\n"
    "MOONCAKE_SPDIAG_SYSTEM_LIBRARY=${library}\n"
    "MOONCAKE_SPDIAG_SYSTEM_CLI=${cli}\n"
    "MOONCAKE_SPDIAG_SYSTEM_CONFIG=${config}\n")
  set(MOONCAKE_SPDIAG_ACTIVE_LAYER
      "${layer}"
      CACHE INTERNAL "Active Mooncake SpDiag layer" FORCE)
endfunction()

if(NOT MOONCAKE_ENABLE_SPDIAG)
  set(MOONCAKE_SPDIAG_GIT_REPOSITORY
      "https://gitcode.com/openeuler/spdiag.git"
      CACHE STRING "SpDiag repository used by Layer 0")
  set(MOONCAKE_SPDIAG_GIT_TAG
      "46b7b84371a6935eaca93763fbb7cb3b4bccfbe2"
      CACHE STRING "SpDiag revision used by Layer 0")
  set(MOONCAKE_SPDIAG_SOURCE_DIR
      ""
      CACHE PATH "Local SpDiag source directory for offline builds")

  include(FetchContent)
  if(MOONCAKE_SPDIAG_SOURCE_DIR)
    set(spdiag_SOURCE_DIR "${MOONCAKE_SPDIAG_SOURCE_DIR}")
  else()
    FetchContent_Populate(
      spdiag
      GIT_REPOSITORY "${MOONCAKE_SPDIAG_GIT_REPOSITORY}"
      GIT_TAG "${MOONCAKE_SPDIAG_GIT_TAG}")
  endif()

  add_library(mooncake_spdiag_mock INTERFACE)
  target_include_directories(mooncake_spdiag_mock
                             INTERFACE "${spdiag_SOURCE_DIR}/include")
  target_compile_definitions(mooncake_spdiag_mock INTERFACE SPDIAG_DISABLE)
  add_library(SpDiag::spdiag_lib ALIAS mooncake_spdiag_mock)

  set(MOONCAKE_SPDIAG_LIBRARY_DIR
      ""
      CACHE INTERNAL "System SpDiag library directory" FORCE)
  _mooncake_write_spdiag_manifest("mock" "" "" "")
  message(STATUS "SpDiag: Layer 0 SPDIAG_DISABLE headers")
  return()
endif()

find_package(SpDiag CONFIG QUIET)
if(NOT SpDiag_FOUND OR NOT TARGET SpDiag::spdiag_lib)
  message(
    FATAL_ERROR
      "MOONCAKE_ENABLE_SPDIAG=ON requires the SpDiag runtime and development "
      "RPMs. Install the RPMs that provide libspdiag.so, the spdiag CLI, and "
      "SpDiagConfig.cmake before configuring Mooncake.")
endif()
set_property(TARGET SpDiag::spdiag_lib PROPERTY IMPORTED_GLOBAL TRUE)

get_target_property(_MOONCAKE_SPDIAG_TARGET_TYPE SpDiag::spdiag_lib TYPE)
if(NOT _MOONCAKE_SPDIAG_TARGET_TYPE STREQUAL "SHARED_LIBRARY")
  message(
    FATAL_ERROR "MOONCAKE_ENABLE_SPDIAG=ON requires a shared libspdiag.so. "
                "Reinstall SpDiag with SPDIAG_BUILD_SHARED=ON.")
endif()

function(_mooncake_get_imported_location target output_variable)
  get_target_property(_configs "${target}" IMPORTED_CONFIGURATIONS)
  foreach(_config IN LISTS _configs)
    string(TOUPPER "${_config}" _config_upper)
    get_target_property(_location "${target}"
                        "IMPORTED_LOCATION_${_config_upper}")
    if(_location)
      set("${output_variable}"
          "${_location}"
          PARENT_SCOPE)
      return()
    endif()
  endforeach()

  get_target_property(_location "${target}" IMPORTED_LOCATION)
  set("${output_variable}"
      "${_location}"
      PARENT_SCOPE)
endfunction()

_mooncake_get_imported_location(SpDiag::spdiag_lib
                                MOONCAKE_SPDIAG_SYSTEM_LIBRARY)
if(NOT MOONCAKE_SPDIAG_SYSTEM_LIBRARY)
  message(FATAL_ERROR "The installed SpDiag package does not expose its "
                      "shared-library location.")
endif()
get_filename_component(MOONCAKE_SPDIAG_SYSTEM_LIBRARY
                       "${MOONCAKE_SPDIAG_SYSTEM_LIBRARY}" REALPATH)
get_filename_component(MOONCAKE_SPDIAG_LIBRARY_DIR
                       "${MOONCAKE_SPDIAG_SYSTEM_LIBRARY}" DIRECTORY)

get_target_property(_MOONCAKE_SPDIAG_DEFINITIONS SpDiag::spdiag_lib
                    INTERFACE_COMPILE_DEFINITIONS)
foreach(_definition SPDIAG_ENABLE_PERCENTILE SPDIAG_ENABLE_PERFLOG)
  if(NOT _definition IN_LIST _MOONCAKE_SPDIAG_DEFINITIONS)
    message(
      FATAL_ERROR
        "The installed SpDiag package does not provide ${_definition}. "
        "Rebuild and reinstall the SpDiag RPM with percentile and PerfLog "
        "enabled.")
  endif()
endforeach()

get_filename_component(_MOONCAKE_SPDIAG_SYSTEM_PREFIX
                       "${MOONCAKE_SPDIAG_LIBRARY_DIR}" DIRECTORY)
unset(MOONCAKE_SPDIAG_SYSTEM_CLI)
unset(MOONCAKE_SPDIAG_SYSTEM_CLI CACHE)
find_program(
  MOONCAKE_SPDIAG_SYSTEM_CLI
  NAMES spdiag
  PATHS "${_MOONCAKE_SPDIAG_SYSTEM_PREFIX}/bin"
  NO_DEFAULT_PATH)
if(NOT MOONCAKE_SPDIAG_SYSTEM_CLI)
  message(
    FATAL_ERROR "The SpDiag SDK was found, but the spdiag CLI is missing. "
                "Install the matching SpDiag runtime RPM.")
endif()

find_program(_MOONCAKE_RPM_EXECUTABLE NAMES rpm)
if(NOT _MOONCAKE_RPM_EXECUTABLE)
  message(FATAL_ERROR "The rpm command is required to compare the SpDiag "
                      "library and CLI versions.")
endif()

function(_mooncake_get_rpm_identity file_path output_variable)
  execute_process(
    COMMAND "${_MOONCAKE_RPM_EXECUTABLE}" -qf --qf
            "%{VERSION}-%{RELEASE}.%{ARCH}" "${file_path}"
    RESULT_VARIABLE _result
    OUTPUT_VARIABLE _identity
    ERROR_VARIABLE _error
    OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_STRIP_TRAILING_WHITESPACE)
  if(NOT _result EQUAL 0)
    message(FATAL_ERROR "${file_path} is not provided by an installed RPM: "
                        "${_error}")
  endif()
  set("${output_variable}"
      "${_identity}"
      PARENT_SCOPE)
endfunction()

_mooncake_get_rpm_identity("${MOONCAKE_SPDIAG_SYSTEM_LIBRARY}"
                           _MOONCAKE_SPDIAG_LIBRARY_IDENTITY)
_mooncake_get_rpm_identity("${MOONCAKE_SPDIAG_SYSTEM_CLI}"
                           _MOONCAKE_SPDIAG_CLI_IDENTITY)
if(NOT "${_MOONCAKE_SPDIAG_LIBRARY_IDENTITY}" STREQUAL
   "${_MOONCAKE_SPDIAG_CLI_IDENTITY}")
  message(
    FATAL_ERROR
      "libspdiag.so and the spdiag CLI come from different RPM versions: "
      "${_MOONCAKE_SPDIAG_LIBRARY_IDENTITY} and "
      "${_MOONCAKE_SPDIAG_CLI_IDENTITY}. Install one matching SpDiag RPM set.")
endif()

if(EXISTS "/etc/spdiag/spdiag.conf")
  set(MOONCAKE_SPDIAG_SYSTEM_CONFIG "/etc/spdiag/spdiag.conf")
else()
  set(MOONCAKE_SPDIAG_SYSTEM_CONFIG "")
endif()

set(MOONCAKE_SPDIAG_LIBRARY_DIR
    "${MOONCAKE_SPDIAG_LIBRARY_DIR}"
    CACHE INTERNAL "System SpDiag library directory" FORCE)
_mooncake_write_spdiag_manifest(
  "system" "${MOONCAKE_SPDIAG_SYSTEM_LIBRARY}" "${MOONCAKE_SPDIAG_SYSTEM_CLI}"
  "${MOONCAKE_SPDIAG_SYSTEM_CONFIG}")
message(
  STATUS "SpDiag: Layer 1 system RPM ${_MOONCAKE_SPDIAG_LIBRARY_IDENTITY}; "
         "library=${MOONCAKE_SPDIAG_SYSTEM_LIBRARY}; "
         "CLI=${MOONCAKE_SPDIAG_SYSTEM_CLI}")
