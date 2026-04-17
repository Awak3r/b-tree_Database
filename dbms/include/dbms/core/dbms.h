#ifndef COURSEWORK_DBMS_DBMS_H
#define COURSEWORK_DBMS_DBMS_H

#include "catalog.h"

namespace dbms
{

class Dbms
{
public:
    Dbms() = default;

    Catalog& catalog() { return _catalog; }
    const Catalog& catalog() const { return _catalog; }

private:
    Catalog _catalog;
};

}

#endif
