#include "dbms/sql/parser.h"

namespace dbms
{

Statement Parser::parse_statement()
{
    if (match_keyword(Keyword::create_kw)) {
        return parse_create();
    }
    if (match_keyword(Keyword::drop_kw)) {
        return parse_drop();
    }
    if (match_keyword(Keyword::use_kw)) {
        return parse_use();
    }
    throw std::runtime_error("Unexpected token");
}

const Token& Parser::peek() const
{
    if (_pos >= _tokens.size()) {
        return _tokens.back();
    }
    return _tokens[_pos];
}

const Token& Parser::consume()
{
    const Token& t = peek();
    if (_pos < _tokens.size()) {
        _pos += 1;
    }
    return t;
}

bool Parser::match_keyword(Keyword kw)
{
    if (peek().type == TokenType::keyword && peek().keyword == kw) {
        consume();
        return true;
    }
    return false;
}

bool Parser::match_symbol(char c)
{
    if (peek().type == TokenType::symbol && peek().symbol == c) {
        consume();
        return true;
    }
    return false;
}

void Parser::expect_keyword(Keyword kw)
{
    if (!match_keyword(kw)) {
        throw std::runtime_error("Expected keyword");
    }
}

void Parser::expect_symbol(char c)
{
    if (!match_symbol(c)) {
        throw std::runtime_error("Expected symbol");
    }
}

std::string Parser::parse_name()
{
    const Token& t = peek();
    if (t.type == TokenType::identifier || t.type == TokenType::string_literal) {
        consume();
        return t.text;
    }
    throw std::runtime_error("Expected name");
}

Statement Parser::parse_create()
{
    if (match_keyword(Keyword::table_kw)) {
        return parse_create_table();
    }
    if (match_keyword(Keyword::database_kw)) {
        return parse_create_database();
    }
    throw std::runtime_error("Expected TABLE or DATABASE");
}

Statement Parser::parse_drop()
{
    if (match_keyword(Keyword::table_kw)) {
        return parse_drop_table();
    }
    if (match_keyword(Keyword::database_kw)) {
        return parse_drop_database();
    }
    throw std::runtime_error("Expected TABLE or DATABASE");
}

Statement Parser::parse_use()
{
    UseDatabaseStmt stmt{};
    stmt.name = parse_name();
    match_symbol(';');
    return stmt;
}

CreateDatabaseStmt Parser::parse_create_database()
{
    CreateDatabaseStmt stmt{};
    stmt.name = parse_name();
    match_symbol(';');
    return stmt;
}

DropDatabaseStmt Parser::parse_drop_database()
{
    DropDatabaseStmt stmt{};
    stmt.name = parse_name();
    match_symbol(';');
    return stmt;
}

CreateTableStmt Parser::parse_create_table()
{
    CreateTableStmt stmt{};
    stmt.name = parse_name();
    expect_symbol('(');
    stmt.columns.push_back(parse_column_def());
    while (match_symbol(',')) {
        stmt.columns.push_back(parse_column_def());
    }
    expect_symbol(')');
    match_symbol(';');
    return stmt;
}

DropTableStmt Parser::parse_drop_table()
{
    DropTableStmt stmt{};
    stmt.name = parse_name();
    match_symbol(';');
    return stmt;
}

ColumnDef Parser::parse_column_def()
{
    ColumnDef col{};
    col.name = parse_name();
    col.type = parse_type_name();
    col.not_null = false;
    col.indexed = false;
    bool advanced = true;
    while (advanced) {
        advanced = false;
        if (match_keyword(Keyword::not_kw)) {
            expect_keyword(Keyword::null_kw);
            col.not_null = true;
            advanced = true;
        }
        if (match_keyword(Keyword::indexed_kw)) {
            col.indexed = true;
            advanced = true;
        }
    }
    return col;
}

std::string Parser::parse_type_name()
{
    if (match_keyword(Keyword::int_kw)) {
        return "INT";
    }
    if (match_keyword(Keyword::string_kw)) {
        return "STRING";
    }
    if (match_keyword(Keyword::bool_kw)) {
        return "BOOL";
    }
    const Token& t = peek();
    if (t.type == TokenType::identifier) {
        consume();
        std::string upper = t.text;
        for (char& ch : upper) {
            if (ch >= 'a' && ch <= 'z') {
                ch = static_cast<char>(ch - 'a' + 'A');
            }
        }
        return upper;
    }
    throw std::runtime_error("Expected type");
}

}
