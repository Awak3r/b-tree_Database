#include <gtest/gtest.h>
#include "dbms/sql/lexer.h"

using dbms::Keyword;
using dbms::Lexer;
using dbms::TokenType;

TEST(lexerTests, createTable)
{
    Lexer lexer("CREATE TABLE users (id INT, name STRING);");
    auto tokens = lexer.tokenize();
    ASSERT_GT(tokens.size(), 0u);
    EXPECT_EQ(tokens[0].type, TokenType::keyword);
    EXPECT_EQ(tokens[0].keyword, Keyword::create_kw);
    EXPECT_EQ(tokens[1].keyword, Keyword::table_kw);
    EXPECT_EQ(tokens[2].type, TokenType::identifier);
    EXPECT_EQ(tokens[2].text, "users");
    EXPECT_EQ(tokens[3].symbol, '(');
    EXPECT_EQ(tokens[4].type, TokenType::identifier);
    EXPECT_EQ(tokens[4].text, "id");
    EXPECT_EQ(tokens[5].keyword, Keyword::int_kw);
    EXPECT_EQ(tokens[6].symbol, ',');
    EXPECT_EQ(tokens[7].type, TokenType::identifier);
    EXPECT_EQ(tokens[7].text, "name");
    EXPECT_EQ(tokens[8].keyword, Keyword::string_kw);
    EXPECT_EQ(tokens[9].symbol, ')');
    EXPECT_EQ(tokens[10].symbol, ';');
    EXPECT_EQ(tokens.back().type, TokenType::eof_token);
}

TEST(lexerTests, stringLiteral)
{
    Lexer lexer("USE 'mydb';");
    auto tokens = lexer.tokenize();
    ASSERT_GE(tokens.size(), 3u);
    EXPECT_EQ(tokens[0].keyword, Keyword::use_kw);
    EXPECT_EQ(tokens[1].type, TokenType::string_literal);
    EXPECT_EQ(tokens[1].text, "mydb");
    EXPECT_EQ(tokens[2].symbol, ';');
}
