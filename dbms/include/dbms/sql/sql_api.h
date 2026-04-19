

#include <algorithm>
#include <charconv>
#include <filesystem>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include "executor.h"

namespace dbms
{

struct SqlResponse {
    bool ok = false;
    bool is_select = false;
    std::string json;   // только для SELECT
    std::string error;  // текст ошибки
};

class SqlApi {
public:
    explicit SqlApi(Dbms& dbms) : _exec(dbms) {}
    SqlResponse execute_sql(const std::string& sql);
private:
    Executor _exec; // держим сессию, чтобы USE сохранялся между вызовами
};


}