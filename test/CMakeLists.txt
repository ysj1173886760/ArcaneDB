file(GLOB ARCANEDB_TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/*/*.cpp")

include(GoogleTest)

set(VALGRIND_OPTIONS
        --error-exitcode=1                      # if leaks are detected, return nonzero value
        # --gen-suppressions=all  # uncomment for leak suppression syntax
        --leak-check=full                       # detailed leak information
        --soname-synonyms=somalloc=*jemalloc*   # also intercept jemalloc
        --trace-children=yes                    # trace child processes
        --track-origins=yes                     # track origin of uninitialized values
        )

# "make unittests"
##########################################
add_custom_target(show-tests COMMAND ${CMAKE_CTEST_COMMAND} --show-only)
add_custom_target(unittests COMMAND ${CMAKE_CTEST_COMMAND} --verbose)

##########################################
# "make XYZ_test"
##########################################
foreach (arcanedb_test_source ${ARCANEDB_TEST_SOURCES})
    # Create a human readable name.
    get_filename_component(arcanedb_test_filename ${arcanedb_test_source} NAME)
    string(REPLACE ".cpp" "" arcanedb_test_name ${arcanedb_test_filename})

    add_executable(${arcanedb_test_name} ${arcanedb_test_source})

    target_link_libraries(${arcanedb_test_name} gtest_main ${CMAKE_PROJECT_NAME}_lib)

    # Set test target properties and dependencies.
    # Specify the output directory. So we can integrate it into ctest directly
    # using path
    set_target_properties(${arcanedb_test_name}
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
        COMMAND ${arcanedb_test_name}
    )

    # # Add the test under CTest.
    # add_test(${arcanedb_test_name} ${VALGRIND_BIN} ${VALGRIND_OPTIONS}
    # --suppressions=${VALGRIND_SUPPRESSIONS_FILE} ${CMAKE_BINARY_DIR}/test/${arcanedb_test_name}
    # --gtest_color=yes --gtest_output=xml:${CMAKE_BINARY_DIR}/test/unit_${test_name}.xml)
    # add_test(${arcanedb_test_name} ${CMAKE_BINARY_DIR}/test/${arcanedb_test_name} --gtest_color=yes
    #         --gtest_output=xml:${CMAKE_BINARY_DIR}/test/${arcanedb_test_name}.xml)
    gtest_discover_tests(${arcanedb_test_name})

endforeach(arcanedb_test_source ${arcanedb_TEST_SOURCES})