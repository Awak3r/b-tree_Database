#include <gtest/gtest.h>
#include <algorithm>
#include <filesystem>
#include <nlohmann/json.hpp>
#include "dbms/sql/lexer.h"
#include "dbms/sql/parser.h"
#include "dbms/sql/executor.h"

using dbms::Dbms;
using dbms::Executor;
using dbms::Lexer;
using dbms::Parser;

static std::filesystem::path prepare_data_root(const std::string& test_name)
{
    std::filesystem::path root = std::filesystem::temp_directory_path() / ("coursework_dbms_tests_" + test_name);
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);
    return root;
}

static bool run_sql(Executor& exec, const std::string& sql)
{
    Lexer lex(sql);
    Parser parser(lex.tokenize());
    return exec.execute(parser.parse_statement());
}

TEST(executorTests, createDropUse)
{
    Dbms dbms(prepare_data_root("create_drop_use"));
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
    Dbms dbms(prepare_data_root("duplicate_database"));
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
    Dbms dbms(prepare_data_root("create_drop_table"));
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
    Dbms dbms(prepare_data_root("create_table_without_db"));
    Executor exec(dbms);

    Lexer lex("CREATE TABLE users (id INT);\n");
    Parser parser(lex.tokenize());
    EXPECT_FALSE(exec.execute(parser.parse_statement()));
}

TEST(executorTests, invalidColumnType)
{
    Dbms dbms(prepare_data_root("invalid_column_type"));
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

TEST(executorTests, insertWithIndexedAndNotNull)
{
    const std::filesystem::path root = prepare_data_root("insert_indexed_not_null");
    Dbms dbms(root);
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
    }
    {
        Lexer lex("CREATE TABLE users (id INT INDEXED, name STRING, age INT NOT NULL);\n");
        Parser parser(lex.tokenize());
        EXPECT_TRUE(exec.execute(parser.parse_statement()));
    }
    {
        Lexer lex("INSERT INTO users (id, name, age) VALUE (1, \"alice\", 20);\n");
        Parser parser(lex.tokenize());
        EXPECT_TRUE(exec.execute(parser.parse_statement()));
    }
    {
        Lexer lex("INSERT INTO users (id, name, age) VALUE (1, \"bob\", 25);\n");
        Parser parser(lex.tokenize());
        EXPECT_FALSE(exec.execute(parser.parse_statement()));
    }
    {
        Lexer lex("INSERT INTO users (id, name, age) VALUE (2, \"charlie\", NULL);\n");
        Parser parser(lex.tokenize());
        EXPECT_FALSE(exec.execute(parser.parse_statement()));
    }
    {
        Lexer lex("INSERT INTO users (name, age) VALUE (\"delta\", 28);\n");
        Parser parser(lex.tokenize());
        EXPECT_FALSE(exec.execute(parser.parse_statement()));
    }

    EXPECT_TRUE(std::filesystem::exists(root / "test" / "users.tbl"));
    EXPECT_TRUE(std::filesystem::exists(root / "test" / "users__id.idx"));
}

TEST(executorTests, insertWithStringIndexed)
{
    const std::filesystem::path root = prepare_data_root("insert_string_indexed");
    Dbms dbms(root);
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
    }
    {
        Lexer lex("CREATE TABLE tags (name STRING INDEXED);\n");
        Parser parser(lex.tokenize());
        EXPECT_TRUE(exec.execute(parser.parse_statement()));
    }
    {
        Lexer lex("INSERT INTO tags (name) VALUE (\"alpha\");\n");
        Parser parser(lex.tokenize());
        EXPECT_TRUE(exec.execute(parser.parse_statement()));
    }
    {
        Lexer lex("INSERT INTO tags (name) VALUE (\"alpha\");\n");
        Parser parser(lex.tokenize());
        EXPECT_FALSE(exec.execute(parser.parse_statement()));
    }

    EXPECT_TRUE(std::filesystem::exists(root / "test" / "tags__name.idx"));
}

TEST(executorTests, selectStarWithWhereBuildsJson)
{
    const std::filesystem::path root = prepare_data_root("select_star_where_json");
    Dbms dbms(root);
    Executor exec(dbms);

    ASSERT_TRUE(run_sql(exec, "CREATE DATABASE test;\n"));
    ASSERT_TRUE(run_sql(exec, "USE test;\n"));
    ASSERT_TRUE(run_sql(exec, "CREATE TABLE users (id INT INDEXED, name STRING, age INT NOT NULL);\n"));
    ASSERT_TRUE(run_sql(exec, "INSERT INTO users (id, name, age) VALUE (1, \"alice\", 20);\n"));
    ASSERT_TRUE(run_sql(exec, "INSERT INTO users (id, name, age) VALUE (2, \"bob\", 25);\n"));
    ASSERT_TRUE(run_sql(exec, "INSERT INTO users (id, name, age) VALUE (3, NULL, 30);\n"));

    ASSERT_TRUE(run_sql(exec, "SELECT * FROM users WHERE age >= 25;\n"));

    const nlohmann::json result = nlohmann::json::parse(exec.last_select_json());
    ASSERT_TRUE(result.is_array());
    ASSERT_EQ(result.size(), 2u);

    EXPECT_EQ(result[0]["id"], 2);
    EXPECT_EQ(result[0]["name"], "bob");
    EXPECT_EQ(result[0]["age"], 25);

    EXPECT_EQ(result[1]["id"], 3);
    EXPECT_TRUE(result[1]["name"].is_null());
    EXPECT_EQ(result[1]["age"], 30);
}

TEST(executorTests, selectProjectionAliasBuildsJson)
{
    const std::filesystem::path root = prepare_data_root("select_projection_alias_json");
    Dbms dbms(root);
    Executor exec(dbms);

    ASSERT_TRUE(run_sql(exec, "CREATE DATABASE test;\n"));
    ASSERT_TRUE(run_sql(exec, "USE test;\n"));
    ASSERT_TRUE(run_sql(exec, "CREATE TABLE users (id INT INDEXED, name STRING, age INT);\n"));
    ASSERT_TRUE(run_sql(exec, "INSERT INTO users (id, name, age) VALUE (1, \"alice\", 20);\n"));
    ASSERT_TRUE(run_sql(exec, "INSERT INTO users (id, name, age) VALUE (2, \"bob\", 25);\n"));

    ASSERT_TRUE(run_sql(exec, "SELECT id AS user_id, name FROM users WHERE id == 1;\n"));

    const nlohmann::json result = nlohmann::json::parse(exec.last_select_json());
    ASSERT_TRUE(result.is_array());
    ASSERT_EQ(result.size(), 1u);
    ASSERT_TRUE(result[0].contains("user_id"));
    ASSERT_TRUE(result[0].contains("name"));
    ASSERT_FALSE(result[0].contains("id"));

    if (result[0]["user_id"].is_number_integer()) {
        EXPECT_EQ(result[0]["user_id"], 1);
    } else {
        EXPECT_EQ(result[0]["user_id"], "1");
    }
    EXPECT_EQ(result[0]["name"], "alice");
}

TEST(executorTests, selectIndexedIntEqualityAndRange)
{
    const std::filesystem::path root = prepare_data_root("select_indexed_int_equality_range");
    Dbms dbms(root);
    Executor exec(dbms);

    ASSERT_TRUE(run_sql(exec, "CREATE DATABASE test;\n"));
    ASSERT_TRUE(run_sql(exec, "USE test;\n"));
    ASSERT_TRUE(run_sql(exec, "CREATE TABLE users (id INT INDEXED, name STRING);\n"));
    ASSERT_TRUE(run_sql(exec, "INSERT INTO users (id, name) VALUE (1, \"alice\");\n"));
    ASSERT_TRUE(run_sql(exec, "INSERT INTO users (id, name) VALUE (2, \"bob\");\n"));
    ASSERT_TRUE(run_sql(exec, "INSERT INTO users (id, name) VALUE (3, \"carol\");\n"));

    ASSERT_TRUE(run_sql(exec, "SELECT id, name FROM users WHERE id == 2;\n"));
    {
        const nlohmann::json result = nlohmann::json::parse(exec.last_select_json());
        ASSERT_TRUE(result.is_array());
        ASSERT_EQ(result.size(), 1u);
        EXPECT_EQ(result[0]["id"], 2);
        EXPECT_EQ(result[0]["name"], "bob");
    }

    ASSERT_TRUE(run_sql(exec, "SELECT id FROM users WHERE id >= 2;\n"));
    {
        const nlohmann::json result = nlohmann::json::parse(exec.last_select_json());
        ASSERT_TRUE(result.is_array());
        ASSERT_EQ(result.size(), 2u);
        std::vector<int> ids;
        ids.reserve(result.size());
        for (const auto& row : result) {
            ids.push_back(row["id"].get<int>());
        }
        std::sort(ids.begin(), ids.end());
        EXPECT_EQ(ids[0], 2);
        EXPECT_EQ(ids[1], 3);
    }
}

TEST(executorTests, selectIndexedIntWithLiteralOnLeft)
{
    const std::filesystem::path root = prepare_data_root("select_indexed_literal_left");
    Dbms dbms(root);
    Executor exec(dbms);

    ASSERT_TRUE(run_sql(exec, "CREATE DATABASE test;\n"));
    ASSERT_TRUE(run_sql(exec, "USE test;\n"));
    ASSERT_TRUE(run_sql(exec, "CREATE TABLE users (id INT INDEXED, name STRING);\n"));
    ASSERT_TRUE(run_sql(exec, "INSERT INTO users (id, name) VALUE (1, \"alice\");\n"));
    ASSERT_TRUE(run_sql(exec, "INSERT INTO users (id, name) VALUE (2, \"bob\");\n"));
    ASSERT_TRUE(run_sql(exec, "INSERT INTO users (id, name) VALUE (3, \"carol\");\n"));

    ASSERT_TRUE(run_sql(exec, "SELECT id FROM users WHERE 2 <= id;\n"));

    const nlohmann::json result = nlohmann::json::parse(exec.last_select_json());
    ASSERT_TRUE(result.is_array());
    ASSERT_EQ(result.size(), 2u);
    std::vector<int> ids;
    ids.reserve(result.size());
    for (const auto& row : result) {
        ids.push_back(row["id"].get<int>());
    }
    std::sort(ids.begin(), ids.end());
    EXPECT_EQ(ids[0], 2);
    EXPECT_EQ(ids[1], 3);
}

TEST(executorTests, selectFailsForUnknownProjectionColumn)
{
    const std::filesystem::path root = prepare_data_root("select_unknown_projection_column");
    Dbms dbms(root);
    Executor exec(dbms);

    ASSERT_TRUE(run_sql(exec, "CREATE DATABASE test;\n"));
    ASSERT_TRUE(run_sql(exec, "USE test;\n"));
    ASSERT_TRUE(run_sql(exec, "CREATE TABLE users (id INT, name STRING);\n"));
    ASSERT_TRUE(run_sql(exec, "INSERT INTO users (id, name) VALUE (1, \"alice\");\n"));

    EXPECT_FALSE(run_sql(exec, "SELECT missing_column FROM users;\n"));
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
