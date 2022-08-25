cmake_minimum_required (VERSION 3.2)

project(rawdb)

find_package (Seastar REQUIRED)

aux_source_directory(./src/backend SRC_BACKEND_LIST)

aux_source_directory(./src/manager SRC_MANAGER_LIST)

add_executable(rawdb ${SRC_BACKEND_LIST} ${SRC_MANAGER_LIST} ./main.cc )

target_link_libraries (rawdb
  PRIVATE Seastar::seastar)