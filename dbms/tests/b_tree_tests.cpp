#include <gtest/gtest.h>
#include <cstdio>

#include "dbms/index/b_tree_disk_index.h"

static void remove_file(const char* path)
{
    std::remove(path);
}

TEST(bTreeDiskIndexPositiveTests, test0)
{
    const char* path = "/home/study/coursework/dbms/data/btree_index_test0.idx";
    remove_file(path);
    dbms::BTreeDiskIndex<int> index(path);
    dbms::Rid rid1{1, 10};
    dbms::Rid rid2{2, 20};
    EXPECT_TRUE(index.insert(4, rid1));
    EXPECT_TRUE(index.insert(9, rid2));
    dbms::Rid out{};
    EXPECT_TRUE(index.find(4, out));
    EXPECT_EQ(out.page_id, rid1.page_id);
    EXPECT_EQ(out.slot_id, rid1.slot_id);
    EXPECT_TRUE(index.find(9, out));
    EXPECT_EQ(out.page_id, rid2.page_id);
    EXPECT_EQ(out.slot_id, rid2.slot_id);
}

TEST(bTreeDiskIndexPositiveTests, test1)
{
    const char* path = "/home/study/coursework/dbms/data/btree_index_test1.idx";
    remove_file(path);
    dbms::BTreeDiskIndex<int> index(path);
    for (int i = 1; i <= 50; ++i) {
        EXPECT_TRUE(index.insert(i, dbms::Rid{i, i * 2}));
    }
    EXPECT_FALSE(index.insert(25, dbms::Rid{25, 500}));
    dbms::Rid out{};
    EXPECT_TRUE(index.find(25, out));
    EXPECT_EQ(out.page_id, 25);
    EXPECT_EQ(out.slot_id, 50);
}

TEST(bTreeDiskIndexPositiveTests, test2)
{
    const char* path = "/home/study/coursework/dbms/data/btree_index_test2.idx";
    remove_file(path);
    dbms::BTreeDiskIndex<int> index(path);
    for (int i = 1; i <= 30; ++i) {
        EXPECT_TRUE(index.insert(i, dbms::Rid{i, i + 1}));
    }
    for (int i = 2; i <= 30; i += 2) {
        EXPECT_TRUE(index.erase(i));
    }
    dbms::Rid out{};
    for (int i = 1; i <= 30; ++i) {
        if (i % 2 == 0) {
            EXPECT_FALSE(index.find(i, out));
        } else {
            EXPECT_TRUE(index.find(i, out));
            EXPECT_EQ(out.page_id, i);
            EXPECT_EQ(out.slot_id, i + 1);
        }
    }
}

TEST(bTreeDiskIndexPositiveTests, test3)
{
    const char* path = "/home/study/coursework/dbms/data/btree_index_test3.idx";
    remove_file(path);
    dbms::BTreeDiskIndex<int> index(path);
    for (int i = 1; i <= 20; ++i) {
        EXPECT_TRUE(index.insert(i, dbms::Rid{i, i * 3}));
    }
    auto result = index.range(5, 9);
    ASSERT_EQ(result.size(), 5u);
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(result[i].first, 5 + i);
        EXPECT_EQ(result[i].second.page_id, 5 + i);
        EXPECT_EQ(result[i].second.slot_id, (5 + i) * 3);
    }
}

TEST(bTreeDiskIndexPositiveTests, test4)
{
    const char* path = "/home/study/coursework/dbms/data/btree_index_test4.idx";
    remove_file(path);
    {
        dbms::BTreeDiskIndex<int> index(path);
        for (int i = 1; i <= 10; ++i) {
            EXPECT_TRUE(index.insert(i, dbms::Rid{i, i * 5}));
        }
    }
    {
        dbms::BTreeDiskIndex<int> index(path);
        dbms::Rid out{};
        EXPECT_TRUE(index.find(1, out));
        EXPECT_EQ(out.page_id, 1);
        EXPECT_EQ(out.slot_id, 5);
        EXPECT_TRUE(index.find(10, out));
        EXPECT_EQ(out.page_id, 10);
        EXPECT_EQ(out.slot_id, 50);
    }
}

TEST(bTreeDiskIndexNegativeTests, test1)
{
    const char* path = "/home/study/coursework/dbms/data/btree_index_test5.idx";
    remove_file(path);
    dbms::BTreeDiskIndex<int> index(path);
    dbms::Rid out{};
    EXPECT_FALSE(index.find(123, out));
    EXPECT_FALSE(index.erase(123));
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
