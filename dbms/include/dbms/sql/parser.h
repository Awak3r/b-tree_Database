#ifndef COURSEWORK_DBMS_SQL_PARSER_H
#define COURSEWORK_DBMS_SQL_PARSER_H

#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include "statements.h"
#include "token.h"

namespace dbms
{

class Parser
{
public:
    explicit Parser(std::vector<Token> tokens) : _tokens(std::move(tokens)), _pos(0) {}

    Statement parse_statement();

private:
    std::vector<Token> _tokens;
    std::size_t _pos;

    const Token& peek() const;
    const Token& consume();
    bool match_keyword(Keyword kw);
    bool match_symbol(char c);
    void expect_keyword(Keyword kw);
    void expect_symbol(char c);
    std::string parse_name();

    Statement parse_create();
    Statement parse_drop();
    Statement parse_use();
    CreateTableStmt parse_create_table();
    DropTableStmt parse_drop_table();
    CreateDatabaseStmt parse_create_database();
    DropDatabaseStmt parse_drop_database();
    ColumnDef parse_column_def();
    std::string parse_type_name();
};

}

#endif
