#ifndef COURSEWORK_DBMS_SCHEMA_H
#define COURSEWORK_DBMS_SCHEMA_H

#include <string>

namespace dbms
{

struct Column
{
    std::string name;
    std::string type;
    bool not_null;
    bool indexed;
};

}

#endif
