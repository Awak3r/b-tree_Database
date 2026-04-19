#ifndef COURSEWORK_DBMS_SQL_EXECUTOR_H
#define COURSEWORK_DBMS_SQL_EXECUTOR_H

#include <filesystem>
#include <optional>
#include <string>
#include <vector>
#include "statements.h"
#include "../core/dbms.h"

namespace dbms
{

class Executor
{
public:
    explicit Executor(Dbms& dbms) : _dbms(dbms), _current_db() {}

    const std::string& current_db() const { return _current_db; }
    const std::string& last_select_json() const { return _last_select_json; }

    bool execute(const Statement& stmt);

private:
    using row_values_type = std::vector<std::optional<std::string>>;

    Dbms& _dbms;
    std::string _current_db;
    std::vector<std::string> _last_select_columns;
    std::vector<row_values_type> _last_select_rows;
    std::string _last_select_json;
    std::vector<std::string> _last_select_column_types;

    void build_last_select_json();
    bool execute_create_database(const CreateDatabaseStmt& stmt);
    bool execute_drop_database(const DropDatabaseStmt& stmt);
    bool execute_use_database(const UseDatabaseStmt& stmt);
    bool execute_create_table(const CreateTableStmt& stmt);
    bool execute_drop_table(const DropTableStmt& stmt);
    bool execute_insert(const InsertStmt& stmt);
    bool execute_select(const SelectStmt& stmt);

    Database* find_current_database();
    static Table* find_table(Database& db, const std::string& table_name);
    static bool parse_int_strict(const std::string& text, int& out_value);
    static bool parse_bool_strict(const std::string& text, bool& out_value);
    static std::string to_lower_ascii(std::string text);
    std::filesystem::path database_path(const std::string& db_name) const;
    std::filesystem::path table_path(const std::string& db_name, const std::string& table_name) const;
    std::filesystem::path index_path(const std::string& db_name, const std::string& table_name, const std::string& column_name) const;
    bool normalize_insert_row(const Table& table,
                              const InsertStmt& stmt,
                              const std::vector<InsertValue>& raw_row,
                              std::vector<int>& int_index_keys,
                              std::vector<int>& bool_index_keys,
                              std::vector<std::string>& string_index_keys,
                              row_values_type& out_row) const;
    static bool is_type_valid(const std::string& type);
};

}

#endif
