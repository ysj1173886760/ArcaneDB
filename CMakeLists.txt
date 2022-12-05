cmake_minimum_required(VERSION 3.14)
project(ArcaneDB)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -fPIC -Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -Wno-unused-parameter -Wno-attributes")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -ggdb -fsanitize=address -fno-omit-frame-pointer -fno-optimize-sibling-calls")
# set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -fPIC")
# set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fPIC")
# set(CMAKE_STATIC_LINKER_FLAGS "${CMAKE_STATIC_LINKER_FLAGS} -fPIC")

set(GCC_COVERAGE_LINK_FLAGS "-fPIC")
message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS_DEBUG: ${CMAKE_CXX_FLAGS_DEBUG}")
message(STATUS "CMAKE_EXE_LINKER_FLAGS: ${CMAKE_EXE_LINKER_FLAGS}")
message(STATUS "CMAKE_SHARED_LINKER_FLAGS: ${CMAKE_SHARED_LINKER_FLAGS}")

# enable CTest
enable_testing()

add_subdirectory(third_party)

# brpc related
set(BRPC_OUTPUT_PATH ${PROJECT_BINARY_DIR}/third_party/incubator-brpc/output)

include(FindThreads)
include(FindProtobuf)

set(BRPC_INCLUDE_PATH ${BRPC_OUTPUT_PATH}/include)
set(BRPC_LIB_PATH ${BRPC_OUTPUT_PATH}/lib/libbrpc.a)
include_directories(${BRPC_INCLUDE_PATH})
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

set(ARCANEDB_LIBS   ${BRPC_LIB_PATH}
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

set(ARCANEDB_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
set(ARCANEDB_TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/test/include)

include_directories(${ARCANEDB_SRC_INCLUDE_DIR}
                    ${ARCANEDB_TEST_INCLUDE_DIR})
include_directories(BEFORE src)

add_subdirectory(src)
add_subdirectory(test)