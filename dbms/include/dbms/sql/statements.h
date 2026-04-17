#ifndef COURSEWORK_DBMS_SQL_AST_H
#define COURSEWORK_DBMS_SQL_AST_H

#include <string>
#include <variant>
#include <vector>

namespace dbms
{

struct CreateDatabaseStmt
{
    std::string name;
};

struct DropDatabaseStmt
{
    std::string name;
};

struct UseDatabaseStmt
{
    std::string name;
};

struct ColumnDef
{
    std::string name;
    std::string type;
    bool not_null;
    bool indexed;
};

struct CreateTableStmt
{
    std::string name;
    std::vector<ColumnDef> columns;
};

struct DropTableStmt
{
    std::string name;
};

using Statement = std::variant<CreateDatabaseStmt, DropDatabaseStmt, UseDatabaseStmt, CreateTableStmt, DropTableStmt>;

}

#endif
