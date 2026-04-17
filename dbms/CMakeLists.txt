cmake_minimum_required(VERSION 3.25)

project(coursework_dbms LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(dbms_core INTERFACE)

target_include_directories(dbms_core INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

include(FetchContent)

FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(dbms
    src/main.cpp
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
    GTest::gtest
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
    GTest::gtest_main
)

enable_testing()
add_test(NAME b_tree_tests COMMAND dbms_tests)
add_test(NAME sql_lexer_tests COMMAND dbms_sql_tests)
