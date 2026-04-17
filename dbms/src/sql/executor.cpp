#include "dbms/sql/executor.h"
#include <algorithm>

namespace dbms
{

bool Executor::execute(const Statement& stmt)
{
    if (const auto* s = std::get_if<CreateDatabaseStmt>(&stmt)) {
        return execute_create_database(*s);
    }
    if (const auto* s = std::get_if<DropDatabaseStmt>(&stmt)) {
        return execute_drop_database(*s);
    }
    if (const auto* s = std::get_if<UseDatabaseStmt>(&stmt)) {
        return execute_use_database(*s);
    }
    if (const auto* s = std::get_if<CreateTableStmt>(&stmt)) {
        return execute_create_table(*s);
    }
    if (const auto* s = std::get_if<DropTableStmt>(&stmt)) {
        return execute_drop_table(*s);
    }
    return false;
}

bool Executor::execute_create_database(const CreateDatabaseStmt& stmt)
{
    auto& dbs = _dbms.catalog().databases();
    auto it = std::find_if(dbs.begin(), dbs.end(), [&](const Database& db){ return db.name() == stmt.name; });
    if (it != dbs.end()) {
        return false;
    }
    dbs.push_back(Database(stmt.name));
    return true;
}

bool Executor::execute_drop_database(const DropDatabaseStmt& stmt)
{
    auto& dbs = _dbms.catalog().databases();
    auto it = std::find_if(dbs.begin(), dbs.end(), [&](const Database& db){ return db.name() == stmt.name; });
    if (it == dbs.end()) {
        return false;
    }
    dbs.erase(it);
    if (_current_db == stmt.name) {
        _current_db.clear();
    }
    return true;
}

bool Executor::execute_use_database(const UseDatabaseStmt& stmt)
{
    auto& dbs = _dbms.catalog().databases();
    auto it = std::find_if(dbs.begin(), dbs.end(), [&](const Database& db){ return db.name() == stmt.name; });
    if (it == dbs.end()) {
        return false;
    }
    _current_db = stmt.name;
    return true;
}

Database* Executor::find_current_database()
{
    if (_current_db.empty()) {
        return nullptr;
    }
    auto& dbs = _dbms.catalog().databases();
    auto it = std::find_if(dbs.begin(), dbs.end(), [&](const Database& db){ return db.name() == _current_db; });
    if (it == dbs.end()) {
        return nullptr;
    }
    return &(*it);
}

bool Executor::is_type_valid(const std::string& type)
{
    return type == "INT" || type == "STRING" || type == "BOOL";
}

bool Executor::execute_create_table(const CreateTableStmt& stmt)
{
    Database* db = find_current_database();
    if (db == nullptr) {
        return false;
    }
    auto& tables = db->tables();
    auto it = std::find_if(tables.begin(), tables.end(), [&](const Table& t){ return t.name() == stmt.name; });
    if (it != tables.end()) {
        return false;
    }
    std::vector<Column> columns;
    columns.reserve(stmt.columns.size());
    for (const auto& col : stmt.columns) {
        if (!is_type_valid(col.type)) {
            return false;
        }
        Column c{};
        c.name = col.name;
        c.type = col.type;
        c.not_null = col.not_null;
        c.indexed = col.indexed;
        columns.push_back(std::move(c));
    }
    tables.push_back(Table(stmt.name, std::move(columns)));
    return true;
}

bool Executor::execute_drop_table(const DropTableStmt& stmt)
{
    Database* db = find_current_database();
    if (db == nullptr) {
        return false;
    }
    auto& tables = db->tables();
    auto it = std::find_if(tables.begin(), tables.end(), [&](const Table& t){ return t.name() == stmt.name; });
    if (it == tables.end()) {
        return false;
    }
    tables.erase(it);
    return true;
}

}
