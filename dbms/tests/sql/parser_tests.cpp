#include <gtest/gtest.h>
#include "dbms/sql/lexer.h"
#include "dbms/sql/parser.h"

using dbms::ColumnDef;
using dbms::ColumnRef;
using dbms::ComparisonOp;
using dbms::CreateTableStmt;
using dbms::DropTableStmt;
using dbms::InsertStmt;
using dbms::Lexer;
using dbms::Literal;
using dbms::Parser;
using dbms::SelectStmt;
using dbms::Statement;
using dbms::WhereComparison;

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

TEST(parserTests, insert)
{
    Lexer lexer("INSERT INTO users (id, name, active) VALUE (10, \"alice\", true), (11, NULL, false);");
    Parser parser(lexer.tokenize());
    Statement stmt = parser.parse_statement();
    auto* insert = std::get_if<InsertStmt>(&stmt);
    ASSERT_NE(insert, nullptr);
    EXPECT_EQ(insert->table_name, "users");
    ASSERT_EQ(insert->columns.size(), 3u);
    EXPECT_EQ(insert->columns[0], "id");
    EXPECT_EQ(insert->columns[1], "name");
    EXPECT_EQ(insert->columns[2], "active");
    ASSERT_EQ(insert->rows.size(), 2u);
    ASSERT_EQ(insert->rows[0].size(), 3u);
    EXPECT_FALSE(insert->rows[0][0].is_null);
    EXPECT_EQ(insert->rows[0][0].text, "10");
    EXPECT_FALSE(insert->rows[0][1].is_null);
    EXPECT_EQ(insert->rows[0][1].text, "alice");
    EXPECT_FALSE(insert->rows[0][2].is_null);
    EXPECT_EQ(insert->rows[0][2].text, "true");
    ASSERT_EQ(insert->rows[1].size(), 3u);
    EXPECT_TRUE(insert->rows[1][1].is_null);
}

TEST(parserTests, selectStar)
{
    Lexer lexer("SELECT * FROM users;");
    Parser parser(lexer.tokenize());
    Statement stmt = parser.parse_statement();
    auto* select = std::get_if<SelectStmt>(&stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_TRUE(select->projection.is_star);
    EXPECT_TRUE(select->projection.items.empty());
    EXPECT_EQ(select->table_name, "users");
    EXPECT_FALSE(select->where.has_value());
}

TEST(parserTests, selectWithAliasAndWhereComparison)
{
    Lexer lexer("SELECT id AS user_id, age FROM users WHERE age >= 18;");
    Parser parser(lexer.tokenize());
    Statement stmt = parser.parse_statement();
    auto* select = std::get_if<SelectStmt>(&stmt);
    ASSERT_NE(select, nullptr);

    EXPECT_FALSE(select->projection.is_star);
    ASSERT_EQ(select->projection.items.size(), 2u);
    EXPECT_EQ(select->projection.items[0].column_name, "id");
    ASSERT_TRUE(select->projection.items[0].alias.has_value());
    EXPECT_EQ(select->projection.items[0].alias.value(), "user_id");
    EXPECT_EQ(select->projection.items[1].column_name, "age");
    EXPECT_FALSE(select->projection.items[1].alias.has_value());
    EXPECT_EQ(select->table_name, "users");

    ASSERT_TRUE(select->where.has_value());
    auto* where = std::get_if<WhereComparison>(&select->where.value());
    ASSERT_NE(where, nullptr);
    EXPECT_EQ(where->op, ComparisonOp::ge);

    auto* lhs = std::get_if<ColumnRef>(&where->lhs);
    ASSERT_NE(lhs, nullptr);
    EXPECT_EQ(lhs->name, "age");

    auto* rhs = std::get_if<Literal>(&where->rhs);
    ASSERT_NE(rhs, nullptr);
    EXPECT_FALSE(rhs->is_null);
    EXPECT_EQ(rhs->text, "18");
}
