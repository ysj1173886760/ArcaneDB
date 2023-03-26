# TODO(sheep) fix this
set(CMAKE_C_COMPILER "/usr/bin/clang-16")
set(CMAKE_CXX_COMPILER "/usr/bin/clang++-16")

cmake_minimum_required(VERSION 3.13)
project(ArcaneDB)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug" CACHE STRING
        "Using debug build type" FORCE)
endif(NOT CMAKE_BUILD_TYPE)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(C_FLAG_COMMON "")
set(CMAKE_CXX_FLAGS_DEBUG "${C_FLAG_COMMON}")
set(CMAKE_CXX_FLAGS "${C_FLAG_COMMON} -O3 -g -fPIC -fno-omit-frame-pointer -fno-optimize-sibling-calls")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fPIC -Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wno-unused-parameter -Wno-attributes")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g -fsanitize=address -fno-omit-frame-pointer -fno-optimize-sibling-calls")
set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -fPIC -flto -ldl")
# set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fPIC")
# set(CMAKE_STATIC_LINKER_FLAGS "${CMAKE_STATIC_LINKER_FLAGS} -fPIC")

# enable CTest
enable_testing()

add_subdirectory(third_party)

find_package(GTest)

# brpc related
set(BRPC_OUTPUT_PATH ${PROJECT_BINARY_DIR}/third_party/incubator-brpc/output)

include(FindThreads)
include(FindProtobuf)

set(BRPC_INCLUDE_PATH ${BRPC_OUTPUT_PATH}/include)
include_directories(${BRPC_INCLUDE_PATH})
set(BRPC_LIB_PATH ${BRPC_OUTPUT_PATH}/lib)
link_directories(${BRPC_LIB_PATH})
message(STATUS "brpc output path: ${BRPC_OUTPUT_PATH}")
message(STATUS "brpc lib path: ${BRPC_LIB_PATH}")
message(STATUS "brpc include path: ${BRPC_INCLUDE_PATH}")

find_path(GFLAGS_INCLUDE_PATH gflags/gflags.h)
find_library(GFLAGS_LIBRARY NAMES gflags libgflags)
if((NOT GFLAGS_INCLUDE_PATH) OR (NOT GFLAGS_LIBRARY))
    message(FATAL_ERROR "Fail to find gflags")
endif()
include_directories(${GFLAGS_INCLUDE_PATH})

find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
find_library(LEVELDB_LIB NAMES leveldb)
if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
    message(FATAL_ERROR "Fail to find leveldb")
endif()
include_directories(${LEVELDB_INCLUDE_PATH})

find_package(OpenSSL)
include_directories(${OPENSSL_INCLUDE_DIR})

set(ARCANEDB_LIBS   brpc-static
                    ${CMAKE_THREAD_LIBS_INIT}
                    ${GFLAGS_LIBRARY}
                    ${PROTOBUF_LIBRARIES}
                    ${LEVELDB_LIB}
                    ${OPENSSL_CRYPTO_LIBRARY}
                    ${OPENSSL_SSL_LIBRARY})

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(ARCANEDB_LIBS ${ARCANEDB_LIBS}
        pthread
        "-framework CoreFoundation"
        "-framework CoreGraphics"
        "-framework CoreData"
        "-framework CoreText"
        "-framework Security"
        "-framework Foundation"
        "-Wl,-U,_MallocExtension_ReleaseFreeMemory"
        "-Wl,-U,_ProfilerStart"
        "-Wl,-U,_ProfilerStop")
endif()

# brpc end

message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS_DEBUG: ${CMAKE_CXX_FLAGS_DEBUG}")
message(STATUS "CMAKE_EXE_LINKER_FLAGS: ${CMAKE_EXE_LINKER_FLAGS}")
message(STATUS "CMAKE_SHARED_LINKER_FLAGS: ${CMAKE_SHARED_LINKER_FLAGS}")
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
message(STATUS "Compiler info: ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")

# spdlog
include_directories(third_party/spdlog/include)
link_directories(${PROJECT_BINARY_DIR}/third_party/spdlog)
    
set(ARCANEDB_LIBS ${ARCANEDB_LIBS}
                  spdlog::spdlog)

# FIXME: better way to compile.
include_directories(third_party/abseil-cpp)
link_directories(${PROJECT_BINARY_DIR}/third_party/abseil-cpp/absl)

include_directories(third_party/rocksdb/include)
set (ARCANEDB_LIBS ${ARCANEDB_LIBS} rocksdb)

set(ARCANEDB_LIBS ${ARCANEDB_LIBS}
                  jemalloc)

set(ARCANEDB_LIBS ${ARCANEDB_LIBS}
                  absl::base
                  absl::hash
                  absl::raw_hash_set)

set(ARCANEDB_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src)
set(ARCANEDB_TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/test)

include_directories(${ARCANEDB_SRC_INCLUDE_DIR}
                    ${ARCANEDB_TEST_INCLUDE_DIR})
include_directories(BEFORE src)

add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(benchmark)

# clang-format
find_program(CLANG_FORMAT_BIN
    NAMES clang-format)

string(CONCAT ARCANEDB_FORMAT_DIRS
    "${PROJECT_SOURCE_DIR}/src,"
    "${PROJECT_SOURCE_DIR}/test,")

# Port from bustub
# Runs clang format and updates files in place.
add_custom_target(format 
    ${PROJECT_SOURCE_DIR}/dev-tools/run_clang_format.py
    ${CLANG_FORMAT_BIN}
    ${PROJECT_SOURCE_DIR}/dev-tools/clang_format_exclusions.txt
    --source_dirs ${ARCANEDB_FORMAT_DIRS}
    --fix
    --quiet)