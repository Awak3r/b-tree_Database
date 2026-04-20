# CMake generated Testfile for 
# Source directory: /home/study/coursework/dbms
# Build directory: /home/study/coursework/dbms/build
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test([=[b_tree_tests]=] "/home/study/coursework/dbms/build/tests/bin/dbms_tests")
set_tests_properties([=[b_tree_tests]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/study/coursework/dbms/CMakeLists.txt;218;add_test;/home/study/coursework/dbms/CMakeLists.txt;0;")
add_test([=[sql_lexer_tests]=] "/home/study/coursework/dbms/build/tests/bin/dbms_sql_tests")
set_tests_properties([=[sql_lexer_tests]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/study/coursework/dbms/CMakeLists.txt;219;add_test;/home/study/coursework/dbms/CMakeLists.txt;0;")
add_test([=[sql_cli_tests]=] "/home/study/coursework/dbms/build/tests/bin/dbms_cli_tests")
set_tests_properties([=[sql_cli_tests]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/study/coursework/dbms/CMakeLists.txt;220;add_test;/home/study/coursework/dbms/CMakeLists.txt;0;")
add_test([=[all_tests]=] "/home/study/coursework/dbms/build/tests/bin/dbms_all_tests")
set_tests_properties([=[all_tests]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/study/coursework/dbms/CMakeLists.txt;221;add_test;/home/study/coursework/dbms/CMakeLists.txt;0;")
subdirs("_deps/googletest-build")
subdirs("_deps/nlohmann_json-build")
subdirs("_deps/grpc-build")
