file(GLOB ARCANEDB_BENCHMARK_SOURCES "${PROJECT_SOURCE_DIR}/benchmark/*.cpp")

##########################################
# "make XYZ_bench"
##########################################
foreach (arcanedb_benchmark_source ${ARCANEDB_BENCHMARK_SOURCES})
    # Create a human readable name.
    get_filename_component(arcanedb_benchmark_filename ${arcanedb_benchmark_source} NAME)
    string(REPLACE ".cpp" "" arcanedb_benchmark_name ${arcanedb_benchmark_filename})

    add_executable(${arcanedb_benchmark_name} ${arcanedb_benchmark_source})

    target_link_libraries(${arcanedb_benchmark_name} 
                                    ${CMAKE_PROJECT_NAME}_lib
                                    ${ARCANEDB_LIBS})

    set_target_properties(${arcanedb_benchmark_name}
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/benchmark"
        COMMAND ${arcanedb_benchmark_name}
    )

endforeach(arcanedb_benchmark_source ${ARCANEDB_BENCHMARK_SOURCES})