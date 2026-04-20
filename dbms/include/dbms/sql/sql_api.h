#ifndef COURSEWORK_DBMS_SQL_API_H
#define COURSEWORK_DBMS_SQL_API_H

#include <string>

#include "dbms/sql/executor.h"

namespace dbms
{

struct SqlResponse
{
    bool ok = false;
    bool is_select = false;
    std::string json;
    std::string error;
};

class SqlApi
{
public:
    explicit SqlApi(Dbms& dbms) : _exec(dbms) {}

    SqlResponse execute_sql(const std::string& sql);

private:
    Executor _exec;
};

}

#endif
