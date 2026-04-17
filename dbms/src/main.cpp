#include <iostream>
#include "dbms/all.h"

int main(int argc, char** argv)
{
    (void)argc;
    (void)argv;
    dbms::TableStorage storage("/home/study/coursework/dbms/data/sample.tbl");
    dbms::Page page;
    int slot = page.append_record(reinterpret_cast<const unsigned char*>("test"), 4);
    storage.write_page(0, page);

    dbms::Page read_back;
    storage.read_page(0, read_back);
    auto bytes = read_back.read_record(slot);
    std::string value(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    std::cout << "Read back: " << value << std::endl;
    return 0;
}
