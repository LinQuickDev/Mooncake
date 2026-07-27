if(NOT DEFINED INPUT_FILE OR INPUT_FILE STREQUAL "")
    message(FATAL_ERROR "INPUT_FILE is required")
endif()

if(NOT EXISTS "${INPUT_FILE}")
    message(FATAL_ERROR "ELF file does not exist: ${INPUT_FILE}")
endif()

file(RPATH_REMOVE FILE "${INPUT_FILE}")
