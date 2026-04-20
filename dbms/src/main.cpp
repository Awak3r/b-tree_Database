#include <iostream>
#include <string>

#include "dbms/core/dbms.h"
#include "dbms/sql/sql_api.h"

int main(int argc, char** argv)
{
    (void)argc;
    (void)argv;
    dbms::Dbms dbms_engine;
    dbms::SqlApi api(dbms_engine);

    std::cout << "DBMS SQL REPL\n";
    std::cout << "Type one SQL statement per line. Use 'exit' or 'quit' to stop.\n";

    std::string line;
    while (true) {
        std::cout << "> ";
        if (!std::getline(std::cin, line)) {
            break;
        }
        if (line == "exit;" || line == "quit;") {
            break;
        }
        if (line.empty()) {
            continue;
        }

        const dbms::SqlResponse response = api.execute_sql(line);
        if (!response.ok) {
            std::cout << "ERROR: " << response.error << '\n';
            continue;
        }
        if (response.is_select) {
            std::cout << response.json << '\n';
            continue;
        }
        std::cout << "OK\n";
    }

    return 0;
}
