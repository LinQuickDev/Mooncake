# Two-layer UbDiag integration for Mooncake.
#
# Layer 0 (default) uses the UbDiag public headers with UBDIAG_DISABLE. Layer 1
# consumes a shared UbDiag SDK and CLI installed from system RPMs.

include_guard(GLOBAL)

option(MOONCAKE_ENABLE_UBDIAG
       "Use the system-installed UbDiag shared library and CLI" OFF)

function(_mooncake_write_ubdiag_manifest layer library cli config)
  file(
    WRITE "${CMAKE_BINARY_DIR}/mooncake_ubdiag.env"
    "MOONCAKE_UBDIAG_LAYER=${layer}\n"
    "MOONCAKE_UBDIAG_SYSTEM_LIBRARY=${library}\n"
    "MOONCAKE_UBDIAG_SYSTEM_CLI=${cli}\n"
    "MOONCAKE_UBDIAG_SYSTEM_CONFIG=${config}\n")
  set(MOONCAKE_UBDIAG_ACTIVE_LAYER
      "${layer}"
      CACHE INTERNAL "Active Mooncake UbDiag layer" FORCE)
endfunction()

if(NOT MOONCAKE_ENABLE_UBDIAG)
  set(MOONCAKE_UBDIAG_GIT_REPOSITORY
      "https://github.com/LinQuickDev/ubdiag.git"
      CACHE STRING "UbDiag repository used by Layer 0")
  set(MOONCAKE_UBDIAG_GIT_TAG
      "8df2c2844d402e2e4dcd5ceab2424e8d36c5f99f"
      CACHE STRING "UbDiag revision used by Layer 0")
  set(MOONCAKE_UBDIAG_SOURCE_DIR
      ""
      CACHE PATH "Local UbDiag source directory for offline builds")

  include(FetchContent)
  if(MOONCAKE_UBDIAG_SOURCE_DIR)
    set(ubdiag_SOURCE_DIR "${MOONCAKE_UBDIAG_SOURCE_DIR}")
  else()
    FetchContent_Populate(
      ubdiag
      GIT_REPOSITORY "${MOONCAKE_UBDIAG_GIT_REPOSITORY}"
      GIT_TAG "${MOONCAKE_UBDIAG_GIT_TAG}")
  endif()

  add_library(mooncake_ubdiag_mock INTERFACE)
  target_include_directories(mooncake_ubdiag_mock
                             INTERFACE "${ubdiag_SOURCE_DIR}/include")
  target_compile_definitions(mooncake_ubdiag_mock INTERFACE UBDIAG_DISABLE)
  add_library(UbDiag::ubdiag_lib ALIAS mooncake_ubdiag_mock)

  set(MOONCAKE_UBDIAG_LIBRARY_DIR
      ""
      CACHE INTERNAL "System UbDiag library directory" FORCE)
  _mooncake_write_ubdiag_manifest("mock" "" "" "")
  message(STATUS "UbDiag: Layer 0 UBDIAG_DISABLE headers")
  return()
endif()

find_package(UbDiag CONFIG QUIET)
if(NOT UbDiag_FOUND OR NOT TARGET UbDiag::ubdiag_lib)
  message(
    FATAL_ERROR
      "MOONCAKE_ENABLE_UBDIAG=ON requires the UbDiag runtime and development "
      "RPMs. Install the RPMs that provide libubdiag.so, the ubdiag CLI, and "
      "UbDiagConfig.cmake before configuring Mooncake.")
endif()
set_property(TARGET UbDiag::ubdiag_lib PROPERTY IMPORTED_GLOBAL TRUE)

get_target_property(_MOONCAKE_UBDIAG_TARGET_TYPE UbDiag::ubdiag_lib TYPE)
if(NOT _MOONCAKE_UBDIAG_TARGET_TYPE STREQUAL "SHARED_LIBRARY")
  message(
    FATAL_ERROR "MOONCAKE_ENABLE_UBDIAG=ON requires a shared libubdiag.so. "
                "Reinstall UbDiag with UBDIAG_BUILD_SHARED=ON.")
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

_mooncake_get_imported_location(UbDiag::ubdiag_lib
                                MOONCAKE_UBDIAG_SYSTEM_LIBRARY)
if(NOT MOONCAKE_UBDIAG_SYSTEM_LIBRARY)
  message(FATAL_ERROR "The installed UbDiag package does not expose its "
                      "shared-library location.")
endif()
get_filename_component(MOONCAKE_UBDIAG_SYSTEM_LIBRARY
                       "${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}" REALPATH)
get_filename_component(MOONCAKE_UBDIAG_LIBRARY_DIR
                       "${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}" DIRECTORY)

get_target_property(_MOONCAKE_UBDIAG_DEFINITIONS UbDiag::ubdiag_lib
                    INTERFACE_COMPILE_DEFINITIONS)
foreach(_definition UBDIAG_ENABLE_PERCENTILE UBDIAG_ENABLE_PERFLOG)
  if(NOT _definition IN_LIST _MOONCAKE_UBDIAG_DEFINITIONS)
    message(
      FATAL_ERROR
        "The installed UbDiag package does not provide ${_definition}. "
        "Rebuild and reinstall the UbDiag RPM with percentile and PerfLog "
        "enabled.")
  endif()
endforeach()

get_filename_component(_MOONCAKE_UBDIAG_SYSTEM_PREFIX
                       "${MOONCAKE_UBDIAG_LIBRARY_DIR}" DIRECTORY)
unset(MOONCAKE_UBDIAG_SYSTEM_CLI)
unset(MOONCAKE_UBDIAG_SYSTEM_CLI CACHE)
find_program(
  MOONCAKE_UBDIAG_SYSTEM_CLI
  NAMES ubdiag
  PATHS "${_MOONCAKE_UBDIAG_SYSTEM_PREFIX}/bin"
  NO_DEFAULT_PATH)
if(NOT MOONCAKE_UBDIAG_SYSTEM_CLI)
  message(
    FATAL_ERROR "The UbDiag SDK was found, but the ubdiag CLI is missing. "
                "Install the matching UbDiag runtime RPM.")
endif()

find_program(_MOONCAKE_RPM_EXECUTABLE NAMES rpm)
if(NOT _MOONCAKE_RPM_EXECUTABLE)
  message(FATAL_ERROR "The rpm command is required to compare the UbDiag "
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

_mooncake_get_rpm_identity("${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}"
                           _MOONCAKE_UBDIAG_LIBRARY_IDENTITY)
_mooncake_get_rpm_identity("${MOONCAKE_UBDIAG_SYSTEM_CLI}"
                           _MOONCAKE_UBDIAG_CLI_IDENTITY)
if(NOT "${_MOONCAKE_UBDIAG_LIBRARY_IDENTITY}" STREQUAL
   "${_MOONCAKE_UBDIAG_CLI_IDENTITY}")
  message(
    FATAL_ERROR
      "libubdiag.so and the ubdiag CLI come from different RPM versions: "
      "${_MOONCAKE_UBDIAG_LIBRARY_IDENTITY} and "
      "${_MOONCAKE_UBDIAG_CLI_IDENTITY}. Install one matching UbDiag RPM set.")
endif()

if(EXISTS "/etc/ubdiag/ubdiag.conf")
  set(MOONCAKE_UBDIAG_SYSTEM_CONFIG "/etc/ubdiag/ubdiag.conf")
else()
  set(MOONCAKE_UBDIAG_SYSTEM_CONFIG "")
endif()

set(MOONCAKE_UBDIAG_LIBRARY_DIR
    "${MOONCAKE_UBDIAG_LIBRARY_DIR}"
    CACHE INTERNAL "System UbDiag library directory" FORCE)
_mooncake_write_ubdiag_manifest(
  "system" "${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}" "${MOONCAKE_UBDIAG_SYSTEM_CLI}"
  "${MOONCAKE_UBDIAG_SYSTEM_CONFIG}")
message(
  STATUS "UbDiag: Layer 1 system RPM ${_MOONCAKE_UBDIAG_LIBRARY_IDENTITY}; "
         "library=${MOONCAKE_UBDIAG_SYSTEM_LIBRARY}; "
         "CLI=${MOONCAKE_UBDIAG_SYSTEM_CLI}")
