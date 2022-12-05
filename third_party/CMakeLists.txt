set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
add_subdirectory(googletest)

set(DOWNLOAD_GTEST OFF CACHE BOOL "" FORCE)
add_subdirectory(incubator-brpc)