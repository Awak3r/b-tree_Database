#include <gtest/gtest.h>
#include "dbms/sql/lexer.h"
#include "dbms/sql/parser.h"

using dbms::ColumnDef;
using dbms::CreateTableStmt;
using dbms::DropTableStmt;
using dbms::Lexer;
using dbms::Parser;
using dbms::Statement;

TEST(parserTests, createTable)
{
    Lexer lexer("CREATE TABLE users (id INT NOT NULL, name STRING INDEXED);");
    Parser parser(lexer.tokenize());
    Statement stmt = parser.parse_statement();
    auto* create = std::get_if<CreateTableStmt>(&stmt);
    ASSERT_NE(create, nullptr);
    EXPECT_EQ(create->name, "users");
    ASSERT_EQ(create->columns.size(), 2u);
    EXPECT_EQ(create->columns[0].name, "id");
    EXPECT_EQ(create->columns[0].type, "INT");
    EXPECT_TRUE(create->columns[0].not_null);
    EXPECT_FALSE(create->columns[0].indexed);
    EXPECT_EQ(create->columns[1].name, "name");
    EXPECT_EQ(create->columns[1].type, "STRING");
    EXPECT_FALSE(create->columns[1].not_null);
    EXPECT_TRUE(create->columns[1].indexed);
}

TEST(parserTests, dropTable)
{
    Lexer lexer("DROP TABLE users;");
    Parser parser(lexer.tokenize());
    Statement stmt = parser.parse_statement();
    auto* drop = std::get_if<DropTableStmt>(&stmt);
    ASSERT_NE(drop, nullptr);
    EXPECT_EQ(drop->name, "users");
}
