# CMake generated Testfile for 
# Source directory: /home/study/coursework/dbms
# Build directory: /home/study/coursework/dbms/build
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test([=[b_tree_tests]=] "/home/study/coursework/dbms/build/dbms_tests")
set_tests_properties([=[b_tree_tests]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/study/coursework/dbms/CMakeLists.txt;81;add_test;/home/study/coursework/dbms/CMakeLists.txt;0;")
add_test([=[sql_lexer_tests]=] "/home/study/coursework/dbms/build/dbms_sql_tests")
set_tests_properties([=[sql_lexer_tests]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/study/coursework/dbms/CMakeLists.txt;82;add_test;/home/study/coursework/dbms/CMakeLists.txt;0;")
subdirs("_deps/googletest-build")
subdirs("_deps/nlohmann_json-build")
