#ifndef COURSEWORK_DBMS_B_TREE_DISK_INDEX_H
#define COURSEWORK_DBMS_B_TREE_DISK_INDEX_H

#include <array>
#include <type_traits>
#include <utility>
#include <vector>

#include "index_page_manager.h"
#include "../storage/record.h"

namespace dbms
{

template<typename tkey, std::size_t t = 5>
class BTreeDiskIndex
{
    static_assert(std::is_trivially_copyable_v<tkey>);

    static constexpr int kMinKeys = static_cast<int>(t - 1);
    static constexpr int kMaxKeys = static_cast<int>(2 * t - 1);
    static constexpr int kMaxChildren = static_cast<int>(2 * t);

    struct Node
    {
        int id;
        int is_leaf;
        int keys_count;
        std::array<tkey, kMaxKeys + 1> keys;
        std::array<Rid, kMaxKeys + 1> values;
        std::array<int, kMaxChildren + 1> children;
    };

public:
    explicit BTreeDiskIndex(std::string path, int order = static_cast<int>(t)) : _pm(std::move(path))
    {
        IndexHeader header{};
        if (!_pm.read_header(header)) {
            header.root_id = -1;
            header.order = order;
            header.next_page_id = 1;
            _pm.write_header(header);
        }
        _header = header;
    }

    bool insert(const tkey& key, const Rid& rid)
    {
        if (_header.root_id < 0) {
            int id = allocate_node(true);
            Node root{};
            read_node(id, root);
            root.keys_count = 1;
            root.keys[0] = key;
            root.values[0] = rid;
            write_node(root);
            _header.root_id = id;
            flush_header();
            return true;
        }
        if (contains(key)) {
            return false;
        }
        Node root{};
        read_node(_header.root_id, root);
        if (root.keys_count == kMaxKeys) {
            int new_root_id = allocate_node(false);
            Node new_root{};
            read_node(new_root_id, new_root);
            new_root.children[0] = root.id;
            split_child(new_root, 0);
            write_node(new_root);
            _header.root_id = new_root_id;
            flush_header();
        }
        insert_non_full(_header.root_id, key, rid);
        return true;
    }

    bool find(const tkey& key, Rid& out_rid) const
    {
        if (_header.root_id < 0) {
            return false;
        }
        int current = _header.root_id;
        while (current >= 0) {
            Node node{};
            if (!read_node(current, node)) {
                return false;
            }
            int idx = 0;
            while (idx < node.keys_count && node.keys[idx] < key) {
                ++idx;
            }
            if (idx < node.keys_count && !(key < node.keys[idx]) && !(node.keys[idx] < key)) {
                out_rid = node.values[idx];
                return true;
            }
            if (node.is_leaf) {
                return false;
            }
            current = node.children[idx];
        }
        return false;
    }

    bool erase(const tkey& key)
    {
        if (_header.root_id < 0) {
            return false;
        }
        bool removed = erase_internal(_header.root_id, key);
        Node root{};
        if (read_node(_header.root_id, root)) {
            if (root.keys_count == 0 && !root.is_leaf) {
                _header.root_id = root.children[0];
                flush_header();
            }
        }
        return removed;
    }

    std::vector<std::pair<tkey, Rid>> range(const tkey& low, const tkey& high) const
    {
        std::vector<std::pair<tkey, Rid>> out;
        if (_header.root_id < 0) {
            return out;
        }
        if (high < low) {
            return out;
        }
        collect_range(_header.root_id, low, high, out);
        return out;
    }

    bool contains(const tkey& key) const
    {
        Rid out{};
        return find(key, out);
    }

private:
    bool read_node(int id, Node& node) const
    {
        if (id < 0) {
            return false;
        }
        return _pm.read_node(id, node);
    }

    void write_node(const Node& node)
    {
        _pm.write_node(node.id, node);
    }

    int allocate_node(bool is_leaf)
    {
        int id = _pm.allocate_page();
        if (id >= _header.next_page_id) {
            _header.next_page_id = id + 1;
        }
        Node node{};
        node.id = id;
        node.is_leaf = is_leaf ? 1 : 0;
        node.keys_count = 0;
        node.children.fill(-1);
        write_node(node);
        flush_header();
        return id;
    }

    void flush_header()
    {
        if (_header.order == 0) {
            _header.order = static_cast<int>(t);
        }
        _pm.write_header(_header);
    }

    int find_key_index(const Node& node, const tkey& key, bool& found) const
    {
        int i = 0;
        while (i < node.keys_count && node.keys[i] < key) {
            ++i;
        }
        found = (i < node.keys_count && !(key < node.keys[i]) && !(node.keys[i] < key));
        return i;
    }

    void split_child(Node& parent, int child_index)
    {
        Node full{};
        read_node(parent.children[child_index], full);
        int right_id = allocate_node(full.is_leaf != 0);
        Node right{};
        read_node(right_id, right);
        const int middle_index = static_cast<int>(t - 1);
        tkey middle_key = full.keys[middle_index];
        Rid middle_val = full.values[middle_index];
        int right_keys = full.keys_count - middle_index - 1;
        for (int i = 0; i < right_keys; ++i) {
            right.keys[i] = full.keys[middle_index + 1 + i];
            right.values[i] = full.values[middle_index + 1 + i];
        }
        right.keys_count = right_keys;
        if (!full.is_leaf) {
            for (int i = 0; i < right_keys + 1; ++i) {
                right.children[i] = full.children[middle_index + 1 + i];
            }
        }
        full.keys_count = middle_index;
        for (int i = parent.keys_count; i > child_index; --i) {
            parent.keys[i] = parent.keys[i - 1];
            parent.values[i] = parent.values[i - 1];
        }
        for (int i = parent.keys_count + 1; i > child_index + 1; --i) {
            parent.children[i] = parent.children[i - 1];
        }
        parent.keys[child_index] = middle_key;
        parent.values[child_index] = middle_val;
        parent.children[child_index + 1] = right.id;
        parent.keys_count += 1;
        write_node(full);
        write_node(right);
    }

    void insert_non_full(int node_id, const tkey& key, const Rid& rid)
    {
        Node node{};
        read_node(node_id, node);
        int i = node.keys_count - 1;
        if (node.is_leaf) {
            while (i >= 0 && key < node.keys[i]) {
                node.keys[i + 1] = node.keys[i];
                node.values[i + 1] = node.values[i];
                --i;
            }
            node.keys[i + 1] = key;
            node.values[i + 1] = rid;
            node.keys_count += 1;
            write_node(node);
            return;
        }
        while (i >= 0 && key < node.keys[i]) {
            --i;
        }
        int child_index = i + 1;
        Node child{};
        read_node(node.children[child_index], child);
        if (child.keys_count == kMaxKeys) {
            split_child(node, child_index);
            write_node(node);
            if (key > node.keys[child_index]) {
                child_index += 1;
            }
        }
        write_node(node);
        insert_non_full(node.children[child_index], key, rid);
    }

    Rid get_predecessor(int node_id, int index, tkey& out_key)
    {
        Node cur{};
        read_node(node_id, cur);
        while (!cur.is_leaf) {
            read_node(cur.children[cur.keys_count], cur);
        }
        out_key = cur.keys[cur.keys_count - 1];
        return cur.values[cur.keys_count - 1];
    }

    Rid get_successor(int node_id, int index, tkey& out_key)
    {
        Node cur{};
        read_node(node_id, cur);
        while (!cur.is_leaf) {
            read_node(cur.children[0], cur);
        }
        out_key = cur.keys[0];
        return cur.values[0];
    }

    void merge_children(Node& node, int index)
    {
        Node left{};
        Node right{};
        read_node(node.children[index], left);
        read_node(node.children[index + 1], right);
        left.keys[left.keys_count] = node.keys[index];
        left.values[left.keys_count] = node.values[index];
        left.keys_count += 1;
        for (int i = 0; i < right.keys_count; ++i) {
            left.keys[left.keys_count] = right.keys[i];
            left.values[left.keys_count] = right.values[i];
            left.keys_count += 1;
        }
        if (!right.is_leaf) {
            for (int i = 0; i <= right.keys_count; ++i) {
                left.children[left.keys_count - right.keys_count + i] = right.children[i];
            }
        }
        for (int i = index; i < node.keys_count - 1; ++i) {
            node.keys[i] = node.keys[i + 1];
            node.values[i] = node.values[i + 1];
            node.children[i + 1] = node.children[i + 2];
        }
        node.keys_count -= 1;
        write_node(left);
        write_node(right);
    }

    bool erase_internal(int node_id, const tkey& key)
    {
        Node node{};
        read_node(node_id, node);
        bool found = false;
        int idx = find_key_index(node, key, found);
        if (found) {
            if (node.is_leaf) {
                for (int i = idx; i < node.keys_count - 1; ++i) {
                    node.keys[i] = node.keys[i + 1];
                    node.values[i] = node.values[i + 1];
                }
                node.keys_count -= 1;
                write_node(node);
                return true;
            }
            Node left{};
            Node right{};
            read_node(node.children[idx], left);
            read_node(node.children[idx + 1], right);
            if (left.keys_count >= static_cast<int>(t)) {
                tkey pred_key{};
                Rid pred_val = get_predecessor(node.children[idx], idx, pred_key);
                node.keys[idx] = pred_key;
                node.values[idx] = pred_val;
                write_node(node);
                return erase_internal(node.children[idx], pred_key);
            }
            if (right.keys_count >= static_cast<int>(t)) {
                tkey succ_key{};
                Rid succ_val = get_successor(node.children[idx + 1], idx, succ_key);
                node.keys[idx] = succ_key;
                node.values[idx] = succ_val;
                write_node(node);
                return erase_internal(node.children[idx + 1], succ_key);
            }
            merge_children(node, idx);
            write_node(node);
            return erase_internal(node.children[idx], key);
        }
        if (node.is_leaf) {
            return false;
        }
        Node child{};
        read_node(node.children[idx], child);
        if (child.keys_count == kMinKeys) {
            if (idx > 0) {
                Node left{};
                read_node(node.children[idx - 1], left);
                if (left.keys_count >= static_cast<int>(t)) {
                    for (int i = child.keys_count; i > 0; --i) {
                        child.keys[i] = child.keys[i - 1];
                        child.values[i] = child.values[i - 1];
                    }
                    child.keys[0] = node.keys[idx - 1];
                    child.values[0] = node.values[idx - 1];
                    if (!left.is_leaf) {
                        for (int i = child.keys_count + 1; i > 0; --i) {
                            child.children[i] = child.children[i - 1];
                        }
                        child.children[0] = left.children[left.keys_count];
                    }
                    node.keys[idx - 1] = left.keys[left.keys_count - 1];
                    node.values[idx - 1] = left.values[left.keys_count - 1];
                    left.keys_count -= 1;
                    child.keys_count += 1;
                    write_node(left);
                    write_node(child);
                    write_node(node);
                } else if (idx + 1 <= node.keys_count) {
                    Node right{};
                    read_node(node.children[idx + 1], right);
                    if (right.keys_count >= static_cast<int>(t)) {
                        child.keys[child.keys_count] = node.keys[idx];
                        child.values[child.keys_count] = node.values[idx];
                        if (!right.is_leaf) {
                            child.children[child.keys_count + 1] = right.children[0];
                        }
                        child.keys_count += 1;
                        node.keys[idx] = right.keys[0];
                        node.values[idx] = right.values[0];
                        for (int i = 0; i < right.keys_count - 1; ++i) {
                            right.keys[i] = right.keys[i + 1];
                            right.values[i] = right.values[i + 1];
                        }
                        if (!right.is_leaf) {
                            for (int i = 0; i < right.keys_count; ++i) {
                                right.children[i] = right.children[i + 1];
                            }
                        }
                        right.keys_count -= 1;
                        write_node(right);
                        write_node(child);
                        write_node(node);
                    } else {
                        merge_children(node, idx);
                        write_node(node);
                    }
                }
            } else if (idx + 1 <= node.keys_count) {
                Node right{};
                read_node(node.children[idx + 1], right);
                if (right.keys_count >= static_cast<int>(t)) {
                    child.keys[child.keys_count] = node.keys[idx];
                    child.values[child.keys_count] = node.values[idx];
                    if (!right.is_leaf) {
                        child.children[child.keys_count + 1] = right.children[0];
                    }
                    child.keys_count += 1;
                    node.keys[idx] = right.keys[0];
                    node.values[idx] = right.values[0];
                    for (int i = 0; i < right.keys_count - 1; ++i) {
                        right.keys[i] = right.keys[i + 1];
                        right.values[i] = right.values[i + 1];
                    }
                    if (!right.is_leaf) {
                        for (int i = 0; i < right.keys_count; ++i) {
                            right.children[i] = right.children[i + 1];
                        }
                    }
                    right.keys_count -= 1;
                    write_node(right);
                    write_node(child);
                    write_node(node);
                } else {
                    merge_children(node, idx);
                    write_node(node);
                }
            }
        }
        return erase_internal(node.children[idx], key);
    }

    void collect_range(int node_id, const tkey& low, const tkey& high, std::vector<std::pair<tkey, Rid>>& out) const
    {
        Node node{};
        if (!read_node(node_id, node)) {
            return;
        }
        for (int i = 0; i < node.keys_count; ++i) {
            if (!node.is_leaf) {
                collect_range(node.children[i], low, high, out);
            }
            const tkey& key = node.keys[i];
            if (!(key < low) && !(high < key)) {
                out.push_back({key, node.values[i]});
            }
            if (high < key) {
                return;
            }
        }
        if (!node.is_leaf) {
            collect_range(node.children[node.keys_count], low, high, out);
        }
    }

    IndexPageManager _pm;
    IndexHeader _header{};
};

}

#endif
