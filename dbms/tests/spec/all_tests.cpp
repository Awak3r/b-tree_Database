#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "dbms/core/dbms.h"
#include "dbms/index/index_manager.h"
#include "dbms/sql/cli.h"
#include "dbms/sql/sql_api.h"
#include "dbms/storage/record_codec.h"
#include "dbms/storage/table_page_manager.h"

namespace
{

struct CliRunResult
{
    int exit_code = 0;
    std::string stdout_text;
};

std::filesystem::path prepare_data_root(const std::string& test_name)
{
    const std::filesystem::path root =
        std::filesystem::temp_directory_path() / ("coursework_dbms_tz_point0_" + test_name);
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);
    return root;
}

std::size_t count_occurrences(const std::string& text, const std::string& needle)
{
    if (needle.empty()) {
        return 0;
    }
    std::size_t count = 0;
    std::size_t pos = 0;
    while (true) {
        pos = text.find(needle, pos);
        if (pos == std::string::npos) {
            break;
        }
        ++count;
        pos += needle.size();
    }
    return count;
}

std::optional<nlohmann::json> try_parse_last_json_array(const std::string& stdout_text)
{
    const std::size_t begin = stdout_text.rfind('[');
    const std::size_t end = stdout_text.rfind(']');
    if (begin == std::string::npos || end == std::string::npos || end < begin) {
        return std::nullopt;
    }
    try {
        return nlohmann::json::parse(stdout_text.substr(begin, end - begin + 1));
    } catch (...) {
        return std::nullopt;
    }
}

CliRunResult run_cli_capture(dbms::SqlApi& api,
                             const std::vector<std::string>& args,
                             const std::string& stdin_text = {})
{
    std::vector<std::string> args_copy = args;
    std::vector<char*> argv;
    argv.reserve(args_copy.size());
    for (std::string& arg : args_copy) {
        argv.push_back(arg.data());
    }

    std::ostringstream captured;
    std::istringstream input(stdin_text);

    std::streambuf* old_out = std::cout.rdbuf(captured.rdbuf());
    std::streambuf* old_in = std::cin.rdbuf(input.rdbuf());
    const int exit_code = dbms::run_cli(api, static_cast<int>(argv.size()), argv.data());
    std::cin.rdbuf(old_in);
    std::cout.rdbuf(old_out);

    return CliRunResult{exit_code, captured.str()};
}

CliRunResult run_script(const std::string& test_name, const std::string& script)
{
    const std::filesystem::path root = prepare_data_root(test_name);
    dbms::Dbms engine(root);
    dbms::SqlApi api(engine);

    const std::filesystem::path script_path = root / "script.sql";
    std::ofstream out(script_path);
    if (out.is_open()) {
        out << script;
    }
    out.close();

    return run_cli_capture(api, {"prog", script_path.string()});
}

void expect_sql_ok(dbms::SqlApi& api, const std::string& sql)
{
    const dbms::SqlResponse response = api.execute_sql(sql);
    ASSERT_TRUE(response.ok) << "SQL failed: `" << sql << "`, error: " << response.error;
}

dbms::SqlResponse run_sql(dbms::SqlApi& api, const std::string& sql)
{
    return api.execute_sql(sql);
}

void force_table_header_next_page(const std::filesystem::path& table_file, int next_page_id)
{
    dbms::TablePageManager manager(table_file.string());
    dbms::Page page;
    ASSERT_TRUE(manager.read_page(0, page));
    dbms::TableHeader header{};
    std::memcpy(&header, page.data().data(), sizeof(dbms::TableHeader));
    header.next_page_id = next_page_id;
    std::memcpy(page.data().data(), &header, sizeof(dbms::TableHeader));
    ASSERT_TRUE(manager.write_page(0, page));
}

} // namespace

TEST(tzPoint0Pass, SupportsIntAndStringDataTypes)
{
    dbms::Dbms engine(prepare_data_root("supports_int_string"));
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "CREATE DATABASE test;");
    expect_sql_ok(api, "USE test;");
    expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING);");
    expect_sql_ok(api, "INSERT INTO users (id, name) VALUE (1, \"alice\");");

    const dbms::SqlResponse select = run_sql(api, "SELECT id, name FROM users WHERE id == 1;");
    ASSERT_TRUE(select.ok);
    ASSERT_TRUE(select.is_select);
    const nlohmann::json result = nlohmann::json::parse(select.json);
    ASSERT_TRUE(result.is_array());
    ASSERT_EQ(result.size(), 1u);
    EXPECT_TRUE(result[0]["id"].is_number_integer());
    EXPECT_TRUE(result[0]["name"].is_string());
}

TEST(tzPoint0Pass, SupportsDatabaseHierarchyAndContextSwitch)
{
    dbms::Dbms engine(prepare_data_root("db_hierarchy_context"));
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "CREATE DATABASE db1;");
    expect_sql_ok(api, "CREATE DATABASE db2;");

    expect_sql_ok(api, "USE db1;");
    expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING);");
    expect_sql_ok(api, "INSERT INTO users (id, name) VALUE (1, \"from_db1\");");

    expect_sql_ok(api, "USE db2;");
    expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING);");
    expect_sql_ok(api, "INSERT INTO users (id, name) VALUE (1, \"from_db2\");");

    {
        expect_sql_ok(api, "USE db1;");
        const dbms::SqlResponse select = run_sql(api, "SELECT name FROM users WHERE id == 1;");
        ASSERT_TRUE(select.ok);
        const nlohmann::json result = nlohmann::json::parse(select.json);
        ASSERT_EQ(result.size(), 1u);
        EXPECT_EQ(result[0]["name"], "from_db1");
    }

    {
        expect_sql_ok(api, "USE db2;");
        const dbms::SqlResponse select = run_sql(api, "SELECT name FROM users WHERE id == 1;");
        ASSERT_TRUE(select.ok);
        const nlohmann::json result = nlohmann::json::parse(select.json);
        ASSERT_EQ(result.size(), 1u);
        EXPECT_EQ(result[0]["name"], "from_db2");
    }
}

TEST(tzPoint0Pass, BatchModeWorksWithScriptFile)
{
    const std::string script =
        "CREATE DATABASE test;\n"
        "USE test;\n"
        "CREATE TABLE users (id INT INDEXED, name STRING);\n"
        "INSERT INTO users (id, name) VALUE (1, \"alice\");\n"
        "SELECT id, name FROM users WHERE id == 1;\n";

    const CliRunResult run = run_script("batch_mode_script", script);
    EXPECT_EQ(run.exit_code, 0);
    EXPECT_EQ(count_occurrences(run.stdout_text, "OK\n"), 4u);
    const auto parsed = try_parse_last_json_array(run.stdout_text);
    ASSERT_TRUE(parsed.has_value());
    ASSERT_TRUE(parsed->is_array());
    ASSERT_EQ(parsed->size(), 1u);
}

TEST(tzPoint0Pass, InteractiveModeWorksForSingleLineCommands)
{
    dbms::Dbms engine(prepare_data_root("interactive_single_line"));
    dbms::SqlApi api(engine);
    const std::string input =
        "CREATE DATABASE test;\n"
        "USE test;\n"
        "CREATE TABLE users (id INT INDEXED, name STRING);\n"
        "INSERT INTO users (id, name) VALUE (1, \"alice\");\n"
        "SELECT id, name FROM users WHERE id == 1;\n"
        "exit;\n";

    const CliRunResult run = run_cli_capture(api, {"prog"}, input);
    EXPECT_EQ(run.exit_code, 0);
    EXPECT_EQ(count_occurrences(run.stdout_text, "OK\n"), 4u);
    const auto parsed = try_parse_last_json_array(run.stdout_text);
    ASSERT_TRUE(parsed.has_value());
    ASSERT_TRUE(parsed->is_array());
    ASSERT_EQ(parsed->size(), 1u);
}

TEST(tzPoint0SpecGaps, InteractiveModeShouldSupportMultilineStatementsBySemicolon)
{
    dbms::Dbms engine(prepare_data_root("interactive_multiline"));
    dbms::SqlApi api(engine);
    const std::string input =
        "CREATE DATABASE test;\n"
        "USE test;\n"
        "CREATE TABLE users (\n"
        "  id INT INDEXED,\n"
        "  name STRING\n"
        ");\n"
        "INSERT INTO users (id, name) VALUE (1, \"alice\");\n"
        "SELECT id, name FROM users WHERE id == 1;\n"
        "exit;\n";

    const CliRunResult run = run_cli_capture(api, {"prog"}, input);
    EXPECT_EQ(run.exit_code, 0);
    EXPECT_EQ(run.stdout_text.find("ERROR:"), std::string::npos);
    const auto parsed = try_parse_last_json_array(run.stdout_text);
    ASSERT_TRUE(parsed.has_value());
    ASSERT_EQ(parsed->size(), 1u);
}

TEST(tzPoint0Pass, WritesTableAndIndexFilesToFilesystem)
{
    const std::filesystem::path root = prepare_data_root("writes_files");
    dbms::Dbms engine(root);
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "CREATE DATABASE test;");
    expect_sql_ok(api, "USE test;");
    expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING);");
    expect_sql_ok(api, "INSERT INTO users (id, name) VALUE (1, \"alice\");");

    EXPECT_TRUE(std::filesystem::exists(root / "test" / "users.tbl"));
    EXPECT_TRUE(std::filesystem::exists(root / "test" / "users__id.idx"));
}

TEST(tzPoint0SpecGaps, DataShouldSurviveRestartWithSameDataRoot)
{
    const std::filesystem::path root = prepare_data_root("restart_persistence");
    {
        dbms::Dbms engine(root);
        dbms::SqlApi api(engine);
        expect_sql_ok(api, "CREATE DATABASE test;");
        expect_sql_ok(api, "USE test;");
        expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING);");
        expect_sql_ok(api, "INSERT INTO users (id, name) VALUE (1, \"alice\");");
    }

    {
        dbms::Dbms engine(root);
        dbms::SqlApi api(engine);
        const dbms::SqlResponse use = run_sql(api, "USE test;");
        ASSERT_TRUE(use.ok) << use.error;
        const dbms::SqlResponse select = run_sql(api, "SELECT id, name FROM users WHERE id == 1;");
        ASSERT_TRUE(select.ok) << select.error;
        const nlohmann::json result = nlohmann::json::parse(select.json);
        ASSERT_EQ(result.size(), 1u);
        EXPECT_EQ(result[0]["id"], 1);
        EXPECT_EQ(result[0]["name"], "alice");
    }
}

TEST(tzPoint0Pass, NotNullAndDefaultNullableRulesWork)
{
    dbms::Dbms engine(prepare_data_root("not_null_nullable"));
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "CREATE DATABASE test;");
    expect_sql_ok(api, "USE test;");
    expect_sql_ok(api, "CREATE TABLE users (id INT NOT NULL, name STRING);");

    {
        const dbms::SqlResponse ok_insert = run_sql(api, "INSERT INTO users (id) VALUE (1);");
        EXPECT_TRUE(ok_insert.ok) << ok_insert.error;
    }
    {
        const dbms::SqlResponse bad_insert = run_sql(api, "INSERT INTO users (name) VALUE (\"x\");");
        EXPECT_FALSE(bad_insert.ok);
        EXPECT_FALSE(bad_insert.error.empty());
    }
    {
        const dbms::SqlResponse select = run_sql(api, "SELECT id, name FROM users WHERE id == 1;");
        ASSERT_TRUE(select.ok);
        const nlohmann::json result = nlohmann::json::parse(select.json);
        ASSERT_EQ(result.size(), 1u);
        EXPECT_TRUE(result[0]["name"].is_null());
    }
}

TEST(tzPoint0Pass, IndexedConstraintIsUniqueAndNotNull)
{
    dbms::Dbms engine(prepare_data_root("indexed_unique_not_null"));
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "CREATE DATABASE test;");
    expect_sql_ok(api, "USE test;");
    expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING);");
    expect_sql_ok(api, "INSERT INTO users (id, name) VALUE (1, \"alice\");");

    {
        const dbms::SqlResponse duplicate = run_sql(api, "INSERT INTO users (id, name) VALUE (1, \"dup\");");
        EXPECT_FALSE(duplicate.ok);
    }
    {
        const dbms::SqlResponse null_id = run_sql(api, "INSERT INTO users (id, name) VALUE (NULL, \"bad\");");
        EXPECT_FALSE(null_id.ok);
    }
}

TEST(tzPoint0SpecGaps, IndexedColumnShouldCreateIndexAtTableCreationTime)
{
    const std::filesystem::path root = prepare_data_root("indexed_create_time");
    dbms::Dbms engine(root);
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "CREATE DATABASE test;");
    expect_sql_ok(api, "USE test;");
    expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING);");

    EXPECT_TRUE(std::filesystem::exists(root / "test" / "users__id.idx"));
}

TEST(tzPoint0Pass, ColumnTypeValidationWorks)
{
    dbms::Dbms engine(prepare_data_root("type_validation"));
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "CREATE DATABASE test;");
    expect_sql_ok(api, "USE test;");
    expect_sql_ok(api, "CREATE TABLE users (id INT, name STRING);");

    const dbms::SqlResponse bad_insert = run_sql(api, "INSERT INTO users (id, name) VALUE (\"abc\", \"alice\");");
    EXPECT_FALSE(bad_insert.ok);
    EXPECT_FALSE(bad_insert.error.empty());
}

TEST(tzPoint0Pass, UsesIndexForIndexedCondition)
{
    const std::filesystem::path root = prepare_data_root("index_usage");
    dbms::Dbms engine(root);
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "CREATE DATABASE test;");
    expect_sql_ok(api, "USE test;");
    expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING);");
    expect_sql_ok(api, "INSERT INTO users (id, name) VALUE (1, \"alice\");");

    force_table_header_next_page(root / "test" / "users.tbl", 1);

    {
        const dbms::SqlResponse indexed = run_sql(api, "SELECT id FROM users WHERE id == 1;");
        ASSERT_TRUE(indexed.ok) << indexed.error;
        const nlohmann::json result = nlohmann::json::parse(indexed.json);
        ASSERT_EQ(result.size(), 1u);
        EXPECT_EQ(result[0]["id"], 1);
    }
    {
        const dbms::SqlResponse non_indexed = run_sql(api, "SELECT name FROM users WHERE name == \"alice\";");
        ASSERT_TRUE(non_indexed.ok) << non_indexed.error;
        const nlohmann::json result = nlohmann::json::parse(non_indexed.json);
        EXPECT_EQ(result.size(), 0u);
    }
}

TEST(tzPoint0Pass, IndexStoresRidReferencingTableRecord)
{
    const std::filesystem::path root = prepare_data_root("index_rid_reference");
    dbms::Dbms engine(root);
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "CREATE DATABASE test;");
    expect_sql_ok(api, "USE test;");
    expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING);");
    expect_sql_ok(api, "INSERT INTO users (id, name) VALUE (7, \"alice\");");

    dbms::IndexManager<int> index((root / "test" / "users__id.idx").string());
    dbms::Rid rid{};
    ASSERT_TRUE(index.find(7, rid));

    dbms::TablePageManager manager((root / "test" / "users.tbl").string());
    dbms::Page page;
    ASSERT_TRUE(manager.read_page(rid.page_id, page));
    const std::vector<unsigned char> raw = page.read_record(rid.slot_id);
    ASSERT_FALSE(raw.empty());

    const dbms::Record record = dbms::deserialize_record(raw.data(), 2);
    const auto& values = record.values();
    ASSERT_EQ(values.size(), 2u);
    ASSERT_TRUE(values[0].has_value());
    ASSERT_TRUE(values[1].has_value());
    EXPECT_EQ(values[0].value(), "7");
    EXPECT_EQ(values[1].value(), "alice");
}

TEST(tzPoint0Pass, SyntaxAndSemanticErrorsAreInformative)
{
    dbms::Dbms engine(prepare_data_root("informative_errors"));
    dbms::SqlApi api(engine);

    const dbms::SqlResponse syntax_error = run_sql(api, "SELECT FROM;");
    EXPECT_FALSE(syntax_error.ok);
    EXPECT_FALSE(syntax_error.error.empty());
    EXPECT_NE(syntax_error.error.find("Parser error"), std::string::npos);

    expect_sql_ok(api, "CREATE DATABASE test;");
    expect_sql_ok(api, "USE test;");
    expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING, age INT NOT NULL);");
    expect_sql_ok(api, "INSERT INTO users (id, name, age) VALUE (1, \"alice\", 20);");

    const dbms::SqlResponse semantic_error = run_sql(api, "SELECT * FROM missing;");
    EXPECT_FALSE(semantic_error.ok);
    EXPECT_FALSE(semantic_error.error.empty());
    EXPECT_NE(semantic_error.error.find("Semantic error"), std::string::npos);
    EXPECT_NE(semantic_error.error.find("missing"), std::string::npos);

    const dbms::SqlResponse type_error = run_sql(api, "INSERT INTO users (id, name, age) VALUE (\"bad\", \"bob\", 25);");
    EXPECT_FALSE(type_error.ok);
    EXPECT_NE(type_error.error.find("Type error"), std::string::npos);
    EXPECT_NE(type_error.error.find("id"), std::string::npos);

    const dbms::SqlResponse duplicate_error = run_sql(api, "INSERT INTO users (id, name, age) VALUE (1, \"dup\", 30);");
    EXPECT_FALSE(duplicate_error.ok);
    EXPECT_NE(duplicate_error.error.find("Constraint error"), std::string::npos);
    EXPECT_NE(duplicate_error.error.find("duplicate INDEXED key"), std::string::npos);

    const dbms::SqlResponse where_error = run_sql(api, "SELECT id FROM users WHERE missing == 1;");
    EXPECT_FALSE(where_error.ok);
    EXPECT_NE(where_error.error.find("Semantic error"), std::string::npos);
    EXPECT_NE(where_error.error.find("missing"), std::string::npos);

    const dbms::SqlResponse regex_error = run_sql(api, "SELECT name FROM users WHERE name LIKE \"[\";");
    EXPECT_FALSE(regex_error.ok);
    EXPECT_NE(regex_error.error.find("Runtime error"), std::string::npos);
    EXPECT_NE(regex_error.error.find("invalid LIKE regex"), std::string::npos);

    const dbms::SqlResponse not_null_error = run_sql(api, "UPDATE users SET age = NULL WHERE id == 1;");
    EXPECT_FALSE(not_null_error.ok);
    EXPECT_NE(not_null_error.error.find("Constraint error"), std::string::npos);
    EXPECT_NE(not_null_error.error.find("age"), std::string::npos);

    const std::vector<dbms::SqlResponse> checked_errors = {
        syntax_error,
        semantic_error,
        type_error,
        duplicate_error,
        where_error,
        regex_error,
        not_null_error
    };
    for (const dbms::SqlResponse& response : checked_errors) {
        EXPECT_EQ(response.error.find("Execution failed"), std::string::npos);
    }
}

TEST(tzPoint0Pass, SelectResultIsJsonArray)
{
    dbms::Dbms engine(prepare_data_root("json_array_result"));
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "CREATE DATABASE test;");
    expect_sql_ok(api, "USE test;");
    expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING);");
    expect_sql_ok(api, "INSERT INTO users (id, name) VALUE (1, \"alice\");");

    const dbms::SqlResponse select = run_sql(api, "SELECT id, name FROM users WHERE id == 1;");
    ASSERT_TRUE(select.ok);
    const nlohmann::json parsed = nlohmann::json::parse(select.json);
    ASSERT_TRUE(parsed.is_array());
    ASSERT_EQ(parsed.size(), 1u);
    ASSERT_TRUE(parsed[0].is_object());
}

TEST(tzPoint0Pass, InvalidQueriesDoNotCrashProcess)
{
    dbms::Dbms engine(prepare_data_root("no_crash_invalid_queries"));
    dbms::SqlApi api(engine);

    const std::vector<std::string> queries = {
        "BAD SQL;",
        "SELECT",
        "INSERT INTO",
        "CREATE TABLE t (id BADTYPE);",
        "DELETE FROM missing WHERE id == 1;"
    };

    for (const std::string& query : queries) {
        dbms::SqlResponse response{};
        EXPECT_NO_THROW(response = run_sql(api, query));
        EXPECT_FALSE(response.ok);
        EXPECT_FALSE(response.error.empty());
    }
}

TEST(tzPoint0Pass, KeywordsAreCaseInsensitive)
{
    dbms::Dbms engine(prepare_data_root("keywords_case_insensitive"));
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "create database test;");
    expect_sql_ok(api, "use test;");
    expect_sql_ok(api, "create table users (id int indexed, name string);");
    expect_sql_ok(api, "insert into users (id, name) value (1, \"alice\");");

    const dbms::SqlResponse select = run_sql(api, "select id, name from users where id == 1;");
    EXPECT_TRUE(select.ok) << select.error;
}

TEST(tzPoint0SpecGaps, MixedCaseKeywordInOneWordShouldBeRejected)
{
    dbms::Dbms engine(prepare_data_root("mixed_case_keyword"));
    dbms::SqlApi api(engine);

    const dbms::SqlResponse response = run_sql(api, "CrEaTe DATABASE test;");
    EXPECT_FALSE(response.ok);
}

TEST(tzPoint0SpecGaps, SingleQuotedStringsShouldBeRejected)
{
    dbms::Dbms engine(prepare_data_root("single_quotes"));
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "CREATE DATABASE test;");
    expect_sql_ok(api, "USE test;");
    expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING);");

    const dbms::SqlResponse response = run_sql(api, "INSERT INTO users (id, name) VALUE (1, 'alice');");
    EXPECT_FALSE(response.ok);
}

TEST(tzPoint0Pass, IdentifierRulesWorkForAllowedAndForbiddenNames)
{
    dbms::Dbms engine(prepare_data_root("identifier_rules"));
    dbms::SqlApi api(engine);

    {
        const dbms::SqlResponse bad = run_sql(api, "CREATE DATABASE 1bad;");
        EXPECT_FALSE(bad.ok);
    }
    {
        const dbms::SqlResponse bad = run_sql(api, "CREATE DATABASE \"bad name\";");
        EXPECT_FALSE(bad.ok);
    }

    expect_sql_ok(api, "CREATE DATABASE db_1;");
    expect_sql_ok(api, "USE db_1;");
    expect_sql_ok(api, "CREATE TABLE users_2 (id INT INDEXED, name STRING);");
}

TEST(tzPoint0SpecGaps, QualifiedDatabaseDotTableAccessShouldWork)
{
    dbms::Dbms engine(prepare_data_root("qualified_table_access"));
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "CREATE DATABASE test;");
    expect_sql_ok(api, "USE test;");
    expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING);");
    expect_sql_ok(api, "INSERT INTO users (id, name) VALUE (1, \"alice\");");

    const dbms::SqlResponse response = run_sql(api, "SELECT id, name FROM test.users WHERE id == 1;");
    ASSERT_TRUE(response.ok) << response.error;
    const nlohmann::json result = nlohmann::json::parse(response.json);
    ASSERT_EQ(result.size(), 1u);
}

TEST(tzPoint0Pass, InsertOmittedNullableColumnsBecomeNull)
{
    dbms::Dbms engine(prepare_data_root("insert_default_null"));
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "CREATE DATABASE test;");
    expect_sql_ok(api, "USE test;");
    expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING, age INT);");
    expect_sql_ok(api, "INSERT INTO users (id) VALUE (1);");

    const dbms::SqlResponse select = run_sql(api, "SELECT id, name, age FROM users WHERE id == 1;");
    ASSERT_TRUE(select.ok);
    const nlohmann::json result = nlohmann::json::parse(select.json);
    ASSERT_EQ(result.size(), 1u);
    EXPECT_TRUE(result[0]["name"].is_null());
    EXPECT_TRUE(result[0]["age"].is_null());
}

TEST(tzPoint0Pass, UpdateDeleteAndSelectAliasWork)
{
    dbms::Dbms engine(prepare_data_root("update_delete_alias"));
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "CREATE DATABASE test;");
    expect_sql_ok(api, "USE test;");
    expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING, age INT);");
    expect_sql_ok(api, "INSERT INTO users (id, name, age) VALUE (1, \"alice\", 20), (2, \"bob\", 25);");
    expect_sql_ok(api, "UPDATE users SET age = 26 WHERE id == 2;");
    expect_sql_ok(api, "DELETE FROM users WHERE id == 1;");

    const dbms::SqlResponse select = run_sql(api, "SELECT id AS user_id, age FROM users WHERE id == 2;");
    ASSERT_TRUE(select.ok);
    const nlohmann::json result = nlohmann::json::parse(select.json);
    ASSERT_EQ(result.size(), 1u);
    EXPECT_TRUE(result[0].contains("user_id"));
    EXPECT_EQ(result[0]["user_id"], 2);
    EXPECT_EQ(result[0]["age"], 26);
}

TEST(tzPoint0Pass, ComparisonOperatorsWork)
{
    dbms::Dbms engine(prepare_data_root("comparison_ops"));
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "CREATE DATABASE test;");
    expect_sql_ok(api, "USE test;");
    expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING);");
    expect_sql_ok(api, "INSERT INTO users (id, name) VALUE (1, \"a\"), (2, \"b\"), (3, \"c\");");

    const std::vector<std::pair<std::string, std::size_t>> checks = {
        {"SELECT id FROM users WHERE id == 2;", 1u},
        {"SELECT id FROM users WHERE id != 2;", 2u},
        {"SELECT id FROM users WHERE id < 2;", 1u},
        {"SELECT id FROM users WHERE id > 2;", 1u},
        {"SELECT id FROM users WHERE id <= 2;", 2u},
        {"SELECT id FROM users WHERE id >= 2;", 2u},
    };

    for (const auto& check : checks) {
        const dbms::SqlResponse response = run_sql(api, check.first);
        ASSERT_TRUE(response.ok) << check.first << " error: " << response.error;
        const nlohmann::json result = nlohmann::json::parse(response.json);
        EXPECT_EQ(result.size(), check.second) << check.first;
    }
}

TEST(tzPoint0Pass, StringComparisonIsLexicographic)
{
    dbms::Dbms engine(prepare_data_root("string_lexicographic"));
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "CREATE DATABASE test;");
    expect_sql_ok(api, "USE test;");
    expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING INDEXED);");
    expect_sql_ok(api, "INSERT INTO users (id, name) VALUE (1, \"alice\"), (2, \"bob\"), (3, \"carol\");");

    const dbms::SqlResponse response = run_sql(api, "SELECT name FROM users WHERE name > \"bob\";");
    ASSERT_TRUE(response.ok);
    const nlohmann::json result = nlohmann::json::parse(response.json);
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0]["name"], "carol");
}

TEST(tzPoint0Pass, ConditionSupportsLiteralOnEitherSideAndColumnVsColumn)
{
    dbms::Dbms engine(prepare_data_root("condition_operands"));
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "CREATE DATABASE test;");
    expect_sql_ok(api, "USE test;");
    expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING);");
    expect_sql_ok(api, "INSERT INTO users (id, name) VALUE (1, \"a\"), (2, \"b\"), (3, \"c\");");

    {
        const dbms::SqlResponse response = run_sql(api, "SELECT id FROM users WHERE 2 <= id;");
        ASSERT_TRUE(response.ok);
        const nlohmann::json result = nlohmann::json::parse(response.json);
        EXPECT_EQ(result.size(), 2u);
    }
    {
        const dbms::SqlResponse response = run_sql(api, "SELECT id FROM users WHERE id == id;");
        ASSERT_TRUE(response.ok);
        const nlohmann::json result = nlohmann::json::parse(response.json);
        EXPECT_EQ(result.size(), 3u);
    }
}

TEST(tzPoint0SpecGaps, BetweenConditionShouldWorkAsHalfOpenInterval)
{
    dbms::Dbms engine(prepare_data_root("between_condition"));
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "CREATE DATABASE test;");
    expect_sql_ok(api, "USE test;");
    expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING);");
    expect_sql_ok(api, "INSERT INTO users (id, name) VALUE (1, \"a\"), (2, \"b\"), (3, \"c\"), (4, \"d\");");

    const dbms::SqlResponse response = run_sql(api, "SELECT id FROM users WHERE id BETWEEN 2 AND 4;");
    ASSERT_TRUE(response.ok) << response.error;
    const nlohmann::json result = nlohmann::json::parse(response.json);
    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0]["id"], 2);
    EXPECT_EQ(result[1]["id"], 3);
}

TEST(tzPoint0SpecGaps, LikeConditionShouldSupportRegexPattern)
{
    dbms::Dbms engine(prepare_data_root("like_condition"));
    dbms::SqlApi api(engine);

    expect_sql_ok(api, "CREATE DATABASE test;");
    expect_sql_ok(api, "USE test;");
    expect_sql_ok(api, "CREATE TABLE users (id INT INDEXED, name STRING);");
    expect_sql_ok(api, "INSERT INTO users (id, name) VALUE (1, \"alice\"), (2, \"bob\"), (3, \"anna\");");

    const dbms::SqlResponse response = run_sql(api, "SELECT name FROM users WHERE name LIKE \"a.*\";");
    ASSERT_TRUE(response.ok) << response.error;
    const nlohmann::json result = nlohmann::json::parse(response.json);
    ASSERT_EQ(result.size(), 2u);
}

TEST(tzPoint0SpecGaps, CommandsShouldRequireSemicolonTerminator)
{
    dbms::Dbms engine(prepare_data_root("semicolon_required"));
    dbms::SqlApi api(engine);

    const dbms::SqlResponse response = run_sql(api, "CREATE DATABASE test");
    EXPECT_FALSE(response.ok);
}
