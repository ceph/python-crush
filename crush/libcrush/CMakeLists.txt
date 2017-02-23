cmake_minimum_required(VERSION 2.8.11)

project(libcrush)

include(CheckIncludeFiles)

# must be kept in sync with upstream libcrush/CMakeLists.txt

CHECK_INCLUDE_FILES("inttypes.h" HAVE_INTTYPES_H)
CHECK_INCLUDE_FILES("stdint.h" HAVE_STDINT_H)
CHECK_INCLUDE_FILES("linux/types.h" HAVE_LINUX_TYPES_H)

configure_file(
  ${CMAKE_SOURCE_DIR}/config-h.in.cmake
  ${CMAKE_BINARY_DIR}/acconfig.h
)
