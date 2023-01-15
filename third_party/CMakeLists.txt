set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
add_subdirectory(googletest)

set(DOWNLOAD_GTEST OFF CACHE BOOL "" FORCE)
add_subdirectory(incubator-brpc)

set(ABSL_PROPAGATE_CXX_STD ON CACHE BOOL "" FORCE)
set(ABSL_USE_EXTERNAL_GOOGLETEST ON CACHE BOOL "" FORCE)
add_subdirectory(abseil-cpp)

add_subdirectory(spdlog)