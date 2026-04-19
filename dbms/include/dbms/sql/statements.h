#ifndef COURSEWORK_DBMS_SQL_AST_H
#define COURSEWORK_DBMS_SQL_AST_H

#include <optional>
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

struct InsertValue
{
    bool is_null;
    std::string text;
};

struct InsertStmt
{
    std::string table_name;
    std::vector<std::string> columns;
    std::vector<std::vector<InsertValue>> rows;
};

// ---------- SELECT AST ----------

struct SelectItem
{
    std::string column_name;
    std::optional<std::string> alias; // SELECT id AS user_id
};

struct SelectProjection
{
    bool is_star = false;              // true для SELECT *
    std::vector<SelectItem> items;     // пусто, если is_star == true
};

struct ColumnRef
{
    std::string name;                  // имя колонки
};

struct Literal
{
    bool is_null = false;              // true для NULL
    std::string text;                  // "10", "alice", "true"
};

using Operand = std::variant<ColumnRef, Literal>;

enum class ComparisonOp
{
    eq, // ==
    ne, // !=
    lt, // <
    gt, // >
    le, // <=
    ge  // >=
};

struct WhereComparison
{
    Operand lhs;
    ComparisonOp op;
    Operand rhs;
};

// пока можно не парсить, но держать в AST заранее
struct WhereBetween
{
    Operand value;
    Operand low;
    Operand high;
};

struct WhereLike
{
    Operand value;
    Operand pattern;
};

using WhereCondition = std::variant<WhereComparison, WhereBetween, WhereLike>;

struct SelectStmt
{
    SelectProjection projection;
    std::string table_name;
    std::optional<WhereCondition> where; // нет WHERE => nullopt
};

using Statement = std::variant<
    CreateDatabaseStmt,
    DropDatabaseStmt,
    UseDatabaseStmt,
    CreateTableStmt,
    DropTableStmt,
    InsertStmt,
    SelectStmt>;

} // namespace dbms

#endif
