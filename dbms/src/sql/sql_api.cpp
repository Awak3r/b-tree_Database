#include "dbms/sql/sql_api.h"
#include "dbms/sql/parser.h"
#include "dbms/sql/lexer.h"
namespace dbms
{

SqlResponse SqlApi::execute_sql(const std::string& sql)
{
    SqlResponse resp{};

    if (sql.find_first_not_of(" \t\r\n") == std::string::npos) {
        resp.error = "Empty SQL";
        return resp;
    }

    try {
        Lexer lexer(sql);
        Parser parser(lexer.tokenize());
        Statement stmt = parser.parse_statement();

        const bool is_select = std::holds_alternative<SelectStmt>(stmt);

        if (!_exec.execute(stmt)) {
            resp.error = "Execution failed";
            return resp;
        }

        resp.ok = true;
        resp.is_select = is_select;
        if (is_select) {
            resp.json = _exec.last_select_json();
        }
        return resp;
    } catch (const std::exception& e) {
        resp.error = e.what();
        return resp;
    }
}

}