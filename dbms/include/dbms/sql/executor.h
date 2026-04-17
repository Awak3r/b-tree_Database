#ifndef COURSEWORK_DBMS_SQL_EXECUTOR_H
#define COURSEWORK_DBMS_SQL_EXECUTOR_H

#include <string>
#include "statements.h"
#include "../core/dbms.h"

namespace dbms
{

class Executor
{
public:
    explicit Executor(Dbms& dbms) : _dbms(dbms), _current_db() {}

    const std::string& current_db() const { return _current_db; }

    bool execute(const Statement& stmt);

private:
    Dbms& _dbms;
    std::string _current_db;

    bool execute_create_database(const CreateDatabaseStmt& stmt);
    bool execute_drop_database(const DropDatabaseStmt& stmt);
    bool execute_use_database(const UseDatabaseStmt& stmt);
    bool execute_create_table(const CreateTableStmt& stmt);
    bool execute_drop_table(const DropTableStmt& stmt);

    Database* find_current_database();
    static bool is_type_valid(const std::string& type);
};

}

#endif
