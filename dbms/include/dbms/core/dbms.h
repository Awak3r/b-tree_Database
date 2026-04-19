#ifndef COURSEWORK_DBMS_DBMS_H
#define COURSEWORK_DBMS_DBMS_H

#include <cstdlib>
#include <filesystem>
#include <string>
#include "catalog.h"

namespace dbms
{

class Dbms
{
public:
    explicit Dbms(std::filesystem::path data_root = default_data_root()) : _catalog(), _data_root(std::move(data_root))
    {
        std::error_code ec;
        std::filesystem::create_directories(_data_root, ec);
    }

    Catalog& catalog() { return _catalog; }
    const Catalog& catalog() const { return _catalog; }
    const std::filesystem::path& data_root() const { return _data_root; }

    static std::filesystem::path default_data_root()
    {
        const char* env = std::getenv("DBMS_DATA_ROOT");
        if (env != nullptr && env[0] != '\0') {
            return std::filesystem::path(env);
        }
        return std::filesystem::temp_directory_path() / "coursework_dbms_data";
    }

private:
    Catalog _catalog;
    std::filesystem::path _data_root;
};

}

#endif
