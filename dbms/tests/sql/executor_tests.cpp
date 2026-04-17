#include <gtest/gtest.h>
#include "dbms/sql/lexer.h"
#include "dbms/sql/parser.h"
#include "dbms/sql/executor.h"

using dbms::Dbms;
using dbms::Executor;
using dbms::Lexer;
using dbms::Parser;

TEST(executorTests, createDropUse)
{
    Dbms dbms;
    Executor exec(dbms);

    {
        Lexer lex("CREATE DATABASE test;\n");
        Parser parser(lex.tokenize());
        EXPECT_TRUE(exec.execute(parser.parse_statement()));
    }
    {
        Lexer lex("USE test;\n");
        Parser parser(lex.tokenize());
        EXPECT_TRUE(exec.execute(parser.parse_statement()));
        EXPECT_EQ(exec.current_db(), "test");
    }
    {
        Lexer lex("DROP DATABASE test;\n");
        Parser parser(lex.tokenize());
        EXPECT_TRUE(exec.execute(parser.parse_statement()));
        EXPECT_TRUE(exec.current_db().empty());
    }
}

TEST(executorTests, duplicateDatabase)
{
    Dbms dbms;
    Executor exec(dbms);

    Lexer lex("CREATE DATABASE test;\n");
    Parser parser(lex.tokenize());
    EXPECT_TRUE(exec.execute(parser.parse_statement()));

    Lexer lex2("CREATE DATABASE test;\n");
    Parser parser2(lex2.tokenize());
    EXPECT_FALSE(exec.execute(parser2.parse_statement()));
}

TEST(executorTests, createDropTable)
{
    Dbms dbms;
    Executor exec(dbms);

    Lexer lex0("CREATE DATABASE test;\n");
    Parser parser0(lex0.tokenize());
    EXPECT_TRUE(exec.execute(parser0.parse_statement()));

    Lexer lex1("USE test;\n");
    Parser parser1(lex1.tokenize());
    EXPECT_TRUE(exec.execute(parser1.parse_statement()));

    Lexer lex2("CREATE TABLE users (id INT NOT NULL, name STRING);\n");
    Parser parser2(lex2.tokenize());
    EXPECT_TRUE(exec.execute(parser2.parse_statement()));

    Lexer lex3("DROP TABLE users;\n");
    Parser parser3(lex3.tokenize());
    EXPECT_TRUE(exec.execute(parser3.parse_statement()));
}

TEST(executorTests, createTableWithoutDb)
{
    Dbms dbms;
    Executor exec(dbms);

    Lexer lex("CREATE TABLE users (id INT);\n");
    Parser parser(lex.tokenize());
    EXPECT_FALSE(exec.execute(parser.parse_statement()));
}

TEST(executorTests, invalidColumnType)
{
    Dbms dbms;
    Executor exec(dbms);

    Lexer lex0("CREATE DATABASE test;\n");
    Parser parser0(lex0.tokenize());
    EXPECT_TRUE(exec.execute(parser0.parse_statement()));

    Lexer lex1("USE test;\n");
    Parser parser1(lex1.tokenize());
    EXPECT_TRUE(exec.execute(parser1.parse_statement()));

    Lexer lex2("CREATE TABLE users (id BADTYPE);\n");
    Parser parser2(lex2.tokenize());
    EXPECT_FALSE(exec.execute(parser2.parse_statement()));
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
