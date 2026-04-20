cmake_minimum_required(VERSION 3.25)

project(coursework_dbms LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(TEST_RUNTIME_DIR "${CMAKE_BINARY_DIR}/tests/bin")

add_library(dbms_core INTERFACE)

target_include_directories(dbms_core INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

include(FetchContent)

FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)

FetchContent_Declare(
    nlohmann_json
    URL https://github.com/nlohmann/json/archive/refs/tags/v3.11.3.zip
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest nlohmann_json)

target_link_libraries(dbms_core INTERFACE
    nlohmann_json::nlohmann_json
)

add_executable(dbms
    src/main.cpp
    src/sql/cli.cpp
    src/sql/lexer.cpp
    src/sql/parser.cpp
    src/sql/executor.cpp
    src/sql/sql_api.cpp
)

target_link_libraries(dbms PRIVATE dbms_core)

set_target_properties(dbms PROPERTIES
    OUTPUT_NAME "prog"
)

add_executable(dbms_tests
    tests/b_tree_tests.cpp
)

target_include_directories(dbms_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(dbms_tests PRIVATE
    dbms_core
    GTest::gtest
)

set_target_properties(dbms_tests PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${TEST_RUNTIME_DIR}"
)

add_executable(dbms_sql_tests
    tests/sql/lexer_tests.cpp
    tests/sql/parser_tests.cpp
    tests/sql/executor_tests.cpp
    src/sql/lexer.cpp
    src/sql/parser.cpp
    src/sql/executor.cpp
)

target_include_directories(dbms_sql_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(dbms_sql_tests PRIVATE
    dbms_core
    GTest::gtest_main
)

set_target_properties(dbms_sql_tests PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${TEST_RUNTIME_DIR}"
)

add_executable(dbms_cli_tests
    tests/sql/cli_script_tests.cpp
    src/sql/cli.cpp
    src/sql/sql_api.cpp
    src/sql/lexer.cpp
    src/sql/parser.cpp
    src/sql/executor.cpp
)

target_include_directories(dbms_cli_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(dbms_cli_tests PRIVATE
    dbms_core
    GTest::gtest_main
)

set_target_properties(dbms_cli_tests PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${TEST_RUNTIME_DIR}"
)

enable_testing()
add_test(NAME b_tree_tests COMMAND $<TARGET_FILE:dbms_tests>)
add_test(NAME sql_lexer_tests COMMAND $<TARGET_FILE:dbms_sql_tests>)
add_test(NAME sql_cli_tests COMMAND $<TARGET_FILE:dbms_cli_tests>)

add_custom_target(run_all_tests
    COMMAND ${CMAKE_CTEST_COMMAND} --test-dir ${CMAKE_BINARY_DIR} --output-on-failure
    DEPENDS dbms_tests dbms_sql_tests dbms_cli_tests
    USES_TERMINAL
)
