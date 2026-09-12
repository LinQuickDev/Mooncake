include_guard(GLOBAL)

include(FetchContent)

# URMA is commonly installed in /usr/include/ub or /usr/include. Search
# these local directories first so an installed SDK takes precedence over
# FetchContent. URMA_ROOT and UMDK_ROOT allow an explicitly installed SDK to
# be selected without changing the default search order.
set(_URMA_LOCAL_INCLUDE_DIRS
    ${URMA_ROOT}
    $ENV{URMA_ROOT}
    ${UMDK_ROOT}
    $ENV{UMDK_ROOT}
    /usr/include/ub
    /usr/include)
list(FILTER _URMA_LOCAL_INCLUDE_DIRS EXCLUDE REGEX "^$")

# Use private search variables and clear their cache entries. Otherwise a
# previous configure that fetched UMDK could hide a newly installed local SDK.
unset(_URMA_API_INCLUDE_DIR)
unset(_URMA_API_INCLUDE_DIR CACHE)
unset(_URMA_UBAGG_INCLUDE_DIR)
unset(_URMA_UBAGG_INCLUDE_DIR CACHE)

find_path(
  _URMA_API_INCLUDE_DIR
  NAMES urma_api.h
  PATHS ${_URMA_LOCAL_INCLUDE_DIRS}
  NO_DEFAULT_PATH)
find_path(
  _URMA_UBAGG_INCLUDE_DIR
  NAMES urma_ubagg.h
  PATHS ${_URMA_LOCAL_INCLUDE_DIRS}
  NO_DEFAULT_PATH)

if (_URMA_API_INCLUDE_DIR AND _URMA_UBAGG_INCLUDE_DIR AND
    EXISTS "${_URMA_API_INCLUDE_DIR}/urma_api.h" AND
    EXISTS "${_URMA_UBAGG_INCLUDE_DIR}/urma_ubagg.h")
  set(urma_INCLUDE_DIR ${_URMA_API_INCLUDE_DIR} ${_URMA_UBAGG_INCLUDE_DIR})
  list(REMOVE_DUPLICATES urma_INCLUDE_DIR)
  message(STATUS "Using local URMA headers")
else()
  message(STATUS "Local URMA headers are incomplete; fetching UMDK")

  FetchContent_Declare(
    urma
    GIT_REPOSITORY https://atomgit.com/openeuler/umdk.git
    GIT_TAG        v25.12.0.B081)
  FetchContent_MakeAvailable(urma)

  # UMDK stores the public headers in separate core and bond directories.
  unset(_URMA_API_INCLUDE_DIR CACHE)
  unset(_URMA_UBAGG_INCLUDE_DIR CACHE)
  find_path(
    _URMA_API_INCLUDE_DIR
    NAMES urma_api.h
    PATHS
      ${urma_SOURCE_DIR}/src/urma/lib/urma/core/include
    NO_DEFAULT_PATH)
  find_path(
    _URMA_UBAGG_INCLUDE_DIR
    NAMES urma_ubagg.h
    PATHS
      ${urma_SOURCE_DIR}/src/urma/lib/urma/bond/include
    NO_DEFAULT_PATH)

  if (NOT _URMA_API_INCLUDE_DIR OR
      NOT EXISTS "${_URMA_API_INCLUDE_DIR}/urma_api.h" OR
      NOT _URMA_UBAGG_INCLUDE_DIR OR
      NOT EXISTS "${_URMA_UBAGG_INCLUDE_DIR}/urma_ubagg.h")
    message(FATAL_ERROR
            "URMA headers not found after fetching UMDK: "
            "urma_api.h=${_URMA_API_INCLUDE_DIR}, "
            "urma_ubagg.h=${_URMA_UBAGG_INCLUDE_DIR}")
  endif()

  set(urma_INCLUDE_DIR ${_URMA_API_INCLUDE_DIR} ${_URMA_UBAGG_INCLUDE_DIR})
  list(REMOVE_DUPLICATES urma_INCLUDE_DIR)
  message(STATUS "Using fetched UMDK source: ${urma_SOURCE_DIR}")
endif()

# Keep both names available for existing and conventional CMake callers.
set(URMA_API_INCLUDE_DIR ${_URMA_API_INCLUDE_DIR})
set(URMA_UBAGG_INCLUDE_DIR ${_URMA_UBAGG_INCLUDE_DIR})
set(URMA_INCLUDE_DIRS ${urma_INCLUDE_DIR})
message(STATUS "urma_INCLUDE_DIR: ${urma_INCLUDE_DIR}")
