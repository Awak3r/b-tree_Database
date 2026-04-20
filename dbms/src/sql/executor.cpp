#include "dbms/sql/executor.h"

#include <nlohmann/json.hpp>
#include <algorithm>
#include <charconv>
#include <filesystem>
#include <limits>
#include <system_error>
#include <unordered_map>
#include <unordered_set>

#include "dbms/index/index_manager.h"
#include "dbms/index/string_key.h"
#include "dbms/storage/record_codec.h"
#include "dbms/storage/table_page_manager.h"


namespace dbms
{





namespace
{

constexpr std::size_t kStringIndexKeySize = 240;
using StringIndexKey = FixedStringKey<kStringIndexKeySize>;

template<typename T>
bool compare_with_op(const T& lhs, const T& rhs, ComparisonOp op)
{
    switch (op) {
        case ComparisonOp::eq: return lhs == rhs;
        case ComparisonOp::ne: return !(lhs == rhs);
        case ComparisonOp::lt: return lhs < rhs;
        case ComparisonOp::gt: return rhs < lhs;
        case ComparisonOp::le: return !(rhs < lhs);
        case ComparisonOp::ge: return !(lhs < rhs);
    }
    return false;
}

bool parse_int_local(const std::string& text, int& out)
{
    if (text.empty()) return false;
    const char* begin = text.data();
    const char* end = text.data() + text.size();
    int value = 0;
    auto r = std::from_chars(begin, end, value);
    if (r.ec != std::errc{} || r.ptr != end) return false;
    out = value;
    return true;
}

std::string to_lower_local(std::string s)
{
    for (char& ch : s) {
        if (ch >= 'A' && ch <= 'Z') ch = static_cast<char>(ch - 'A' + 'a');
    }
    return s;
}

bool parse_bool_local(const std::string& text, bool& out)
{
    const std::string lower = to_lower_local(text);
    if (lower == "true" || lower == "1") { out = true; return true; }
    if (lower == "false" || lower == "0") { out = false; return true; }
    return false;
}

struct ResolvedOperand
{
    std::optional<std::string> value;
    std::string type;
    bool has_type = false;
};

struct IndexWherePlan
{
    std::size_t column_idx = 0;
    ComparisonOp op = ComparisonOp::eq;
    std::string literal;
};

ComparisonOp reverse_comparison_op(ComparisonOp op)
{
    switch (op) {
        case ComparisonOp::eq: return ComparisonOp::eq;
        case ComparisonOp::ne: return ComparisonOp::ne;
        case ComparisonOp::lt: return ComparisonOp::gt;
        case ComparisonOp::gt: return ComparisonOp::lt;
        case ComparisonOp::le: return ComparisonOp::ge;
        case ComparisonOp::ge: return ComparisonOp::le;
    }
    return ComparisonOp::eq;
}

bool extract_column_literal_pattern(const WhereComparison& where,
                                    std::string& out_column,
                                    ComparisonOp& out_op,
                                    std::string& out_literal)
{
    const auto* lhs_col = std::get_if<ColumnRef>(&where.lhs);
    const auto* rhs_col = std::get_if<ColumnRef>(&where.rhs);
    const auto* lhs_lit = std::get_if<Literal>(&where.lhs);
    const auto* rhs_lit = std::get_if<Literal>(&where.rhs);

    if (lhs_col != nullptr && rhs_lit != nullptr && !rhs_lit->is_null) {
        out_column = lhs_col->name;
        out_op = where.op;
        out_literal = rhs_lit->text;
        return true;
    }

    if (rhs_col != nullptr && lhs_lit != nullptr && !lhs_lit->is_null) {
        out_column = rhs_col->name;
        out_op = reverse_comparison_op(where.op);
        out_literal = lhs_lit->text;
        return true;
    }

    return false;
}

bool try_build_index_where_plan(const WhereComparison& where,
                                const std::vector<Column>& columns,
                                const std::unordered_map<std::string, std::size_t>& col_pos,
                                IndexWherePlan& out_plan)
{
    std::string column_name;
    ComparisonOp op = ComparisonOp::eq;
    std::string literal;
    if (!extract_column_literal_pattern(where, column_name, op, literal)) {
        return false;
    }

    const auto it = col_pos.find(column_name);
    if (it == col_pos.end()) {
        return false;
    }
    const std::size_t column_idx = it->second;
    if (column_idx >= columns.size()) {
        return false;
    }
    if (!columns[column_idx].indexed) {
        return false;
    }

    out_plan.column_idx = column_idx;
    out_plan.op = op;
    out_plan.literal = std::move(literal);
    return true;
}

bool read_row_by_rid(const TablePageManager& manager,
                     const Rid& rid,
                     std::size_t columns_count,
                     std::vector<std::optional<std::string>>& out_row)
{
    if (rid.page_id < 0 || rid.slot_id < 0) {
        return false;
    }
    Page page;
    if (!manager.read_page(rid.page_id, page)) {
        return false;
    }
    const auto raw = page.read_record(rid.slot_id);
    if (raw.empty()) {
        return false;
    }
    const Record rec = deserialize_record(raw.data(), columns_count);
    out_row = rec.values();
    return true;
}

void collect_int_index_rids(IndexManager<int>& index, int key, ComparisonOp op, std::vector<Rid>& out_rids)
{
    if (op == ComparisonOp::eq) {
        Rid rid{};
        if (index.find(key, rid)) {
            out_rids.push_back(rid);
        }
        return;
    }

    int low = std::numeric_limits<int>::min();
    int high = std::numeric_limits<int>::max();
    if (op == ComparisonOp::lt || op == ComparisonOp::le) {
        high = key;
    } else if (op == ComparisonOp::gt || op == ComparisonOp::ge) {
        low = key;
    }
    const auto pairs = index.range(low, high);
    for (const auto& entry : pairs) {
        if (compare_with_op(entry.first, key, op)) {
            out_rids.push_back(entry.second);
        }
    }
}

void collect_string_index_rids(IndexManager<StringIndexKey>& index,
                               const StringIndexKey& key,
                               ComparisonOp op,
                               std::vector<Rid>& out_rids)
{
    if (op == ComparisonOp::eq) {
        Rid rid{};
        if (index.find(key, rid)) {
            out_rids.push_back(rid);
        }
        return;
    }

    StringIndexKey low{};
    StringIndexKey high{};
    high.bytes.fill(static_cast<char>(0x7F));
    high.length = static_cast<decltype(high.length)>(kStringIndexKeySize);

    if (op == ComparisonOp::lt || op == ComparisonOp::le) {
        high = key;
    } else if (op == ComparisonOp::gt || op == ComparisonOp::ge) {
        low = key;
    }
    const auto pairs = index.range(low, high);
    for (const auto& entry : pairs) {
        if (compare_with_op(entry.first, key, op)) {
            out_rids.push_back(entry.second);
        }
    }
}

bool collect_rids_from_index(const std::filesystem::path& idx_path,
                             const Column& indexed_column,
                             ComparisonOp op,
                             const std::string& literal,
                             bool& out_used_index,
                             std::vector<Rid>& out_rids)
{
    out_used_index = false;
    out_rids.clear();

    if (op == ComparisonOp::ne) {
        return true; // fallback to full scan for !=
    }

    if (indexed_column.type == "INT") {
        int key = 0;
        if (!parse_int_local(literal, key)) {
            return true;
        }
        out_used_index = true;
        IndexManager<int> index(idx_path.string());
        collect_int_index_rids(index, key, op, out_rids);
        return true;
    }

    if (indexed_column.type == "BOOL") {
        bool parsed = false;
        if (!parse_bool_local(literal, parsed)) {
            return true;
        }
        out_used_index = true;
        IndexManager<int> index(idx_path.string());
        collect_int_index_rids(index, parsed ? 1 : 0, op, out_rids);
        return true;
    }

    if (indexed_column.type == "STRING") {
        StringIndexKey key{};
        if (!StringIndexKey::from_string(literal, key)) {
            return true;
        }
        out_used_index = true;
        IndexManager<StringIndexKey> index(idx_path.string());
        collect_string_index_rids(index, key, op, out_rids);
        return true;
    }

    return false;
}

bool resolve_operand_value(const Operand& operand,
                           const std::vector<std::optional<std::string>>& row,
                           const std::vector<Column>& columns,
                           const std::unordered_map<std::string, std::size_t>& col_pos,
                           ResolvedOperand& out)
{
    if (const auto* col = std::get_if<ColumnRef>(&operand)) {
        auto it = col_pos.find(col->name);
        if (it == col_pos.end()) return false;
        std::size_t idx = it->second;
        if (idx >= row.size() || idx >= columns.size()) return false;
        out.value = row[idx];
        out.type = columns[idx].type;
        out.has_type = true;
        return true;
    }

    const auto* lit = std::get_if<Literal>(&operand);
    if (lit == nullptr) return false;
    out.value = lit->is_null ? std::optional<std::string>{} : std::optional<std::string>{lit->text};
    out.type.clear();
    out.has_type = false;
    return true;
}

bool evaluate_where_comparison(const WhereComparison& where,
                               const std::vector<std::optional<std::string>>& row,
                               const std::vector<Column>& columns,
                               const std::unordered_map<std::string, std::size_t>& col_pos)
{
    ResolvedOperand lhs{};
    ResolvedOperand rhs{};
    if (!resolve_operand_value(where.lhs, row, columns, col_pos, lhs)) return false;
    if (!resolve_operand_value(where.rhs, row, columns, col_pos, rhs)) return false;

    if (!lhs.value.has_value() || !rhs.value.has_value()) return false; // NULL -> false

    std::string cmp_type = "STRING";
    if (lhs.has_type && rhs.has_type) {
        if (lhs.type != rhs.type) return false;
        cmp_type = lhs.type;
    } else if (lhs.has_type) {
        cmp_type = lhs.type;
    } else if (rhs.has_type) {
        cmp_type = rhs.type;
    }

    if (cmp_type == "INT") {
        int l = 0, r = 0;
        if (!parse_int_local(*lhs.value, l) || !parse_int_local(*rhs.value, r)) return false;
        return compare_with_op(l, r, where.op);
    }

    if (cmp_type == "BOOL") {
        bool l = false, r = false;
        if (!parse_bool_local(*lhs.value, l) || !parse_bool_local(*rhs.value, r)) return false;
        return compare_with_op(l, r, where.op);
    }

    return compare_with_op(*lhs.value, *rhs.value, where.op);
}


}

bool Executor::execute(const Statement& stmt)
{
    if (const auto* s = std::get_if<CreateDatabaseStmt>(&stmt)) {
        return execute_create_database(*s);
    }
    if (const auto* s = std::get_if<DropDatabaseStmt>(&stmt)) {
        return execute_drop_database(*s);
    }
    if (const auto* s = std::get_if<UseDatabaseStmt>(&stmt)) {
        return execute_use_database(*s);
    }
    if (const auto* s = std::get_if<CreateTableStmt>(&stmt)) {
        return execute_create_table(*s);
    }
    if (const auto* s = std::get_if<DropTableStmt>(&stmt)) {
        return execute_drop_table(*s);
    }
    if (const auto* s = std::get_if<InsertStmt>(&stmt)) {
        return execute_insert(*s);
    }
    if (const auto* s = std::get_if<SelectStmt>(&stmt)) {
        return execute_select(*s);
    }
    if (const auto* s = std::get_if<UpdateStmt>(&stmt)) {
        return execute_update(*s);
    }
    return false;
}

bool Executor::execute_create_database(const CreateDatabaseStmt& stmt)
{
    auto& dbs = _dbms.catalog().databases();
    auto it = std::find_if(dbs.begin(), dbs.end(), [&](const Database& db){ return db.name() == stmt.name; });
    if (it != dbs.end()) {
        return false;
    }
    std::error_code ec;
    std::filesystem::create_directories(database_path(stmt.name), ec);
    if (ec) {
        return false;
    }
    dbs.push_back(Database(stmt.name));
    return true;
}

bool Executor::execute_drop_database(const DropDatabaseStmt& stmt)
{
    auto& dbs = _dbms.catalog().databases();
    auto it = std::find_if(dbs.begin(), dbs.end(), [&](const Database& db){ return db.name() == stmt.name; });
    if (it == dbs.end()) {
        return false;
    }
    std::error_code ec;
    std::filesystem::remove_all(database_path(stmt.name), ec);
    if (ec) {
        return false;
    }
    dbs.erase(it);
    if (_current_db == stmt.name) {
        _current_db.clear();
    }
    return true;
}

bool Executor::execute_use_database(const UseDatabaseStmt& stmt)
{
    auto& dbs = _dbms.catalog().databases();
    auto it = std::find_if(dbs.begin(), dbs.end(), [&](const Database& db){ return db.name() == stmt.name; });
    if (it == dbs.end()) {
        return false;
    }
    _current_db = stmt.name;
    return true;
}

Database* Executor::find_current_database()
{
    if (_current_db.empty()) {
        return nullptr;
    }
    auto& dbs = _dbms.catalog().databases();
    auto it = std::find_if(dbs.begin(), dbs.end(), [&](const Database& db){ return db.name() == _current_db; });
    if (it == dbs.end()) {
        return nullptr;
    }
    return &(*it);
}

Table* Executor::find_table(Database& db, const std::string& table_name)
{
    auto& tables = db.tables();
    auto it = std::find_if(tables.begin(), tables.end(), [&](const Table& table){ return table.name() == table_name; });
    if (it == tables.end()) {
        return nullptr;
    }
    return &(*it);
}

bool Executor::parse_int_strict(const std::string& text, int& out_value)
{
    if (text.empty()) {
        return false;
    }
    int value = 0;
    const char* begin = text.data();
    const char* end = text.data() + text.size();
    auto result = std::from_chars(begin, end, value);
    if (result.ec != std::errc{} || result.ptr != end) {
        return false;
    }
    out_value = value;
    return true;
}

std::string Executor::to_lower_ascii(std::string text)
{
    for (char& ch : text) {
        if (ch >= 'A' && ch <= 'Z') {
            ch = static_cast<char>(ch - 'A' + 'a');
        }
    }
    return text;
}

bool Executor::parse_bool_strict(const std::string& text, bool& out_value)
{
    const std::string lower = to_lower_ascii(text);
    if (lower == "true" || lower == "1") {
        out_value = true;
        return true;
    }
    if (lower == "false" || lower == "0") {
        out_value = false;
        return true;
    }
    return false;
}

std::filesystem::path Executor::database_path(const std::string& db_name) const
{
    return _dbms.data_root() / db_name;
}

std::filesystem::path Executor::table_path(const std::string& db_name, const std::string& table_name) const
{
    return database_path(db_name) / (table_name + ".tbl");
}

std::filesystem::path Executor::index_path(const std::string& db_name, const std::string& table_name, const std::string& column_name) const
{
    return database_path(db_name) / (table_name + "__" + column_name + ".idx");
}

bool Executor::is_type_valid(const std::string& type)
{
    return type == "INT" || type == "STRING" || type == "BOOL";
}

bool Executor::execute_create_table(const CreateTableStmt& stmt)
{
    Database* db = find_current_database();
    if (db == nullptr) {
        return false;
    }
    auto& tables = db->tables();
    auto it = std::find_if(tables.begin(), tables.end(), [&](const Table& t){ return t.name() == stmt.name; });
    if (it != tables.end()) {
        return false;
    }
    std::unordered_set<std::string> names;
    std::vector<Column> columns;
    columns.reserve(stmt.columns.size());
    for (const auto& col : stmt.columns) {
        if (!is_type_valid(col.type)) {
            return false;
        }
        if (!names.insert(col.name).second) {
            return false;
        }
        Column c{};
        c.name = col.name;
        c.type = col.type;
        c.not_null = col.not_null;
        c.indexed = col.indexed;
        columns.push_back(std::move(c));
    }
    std::error_code ec;
    std::filesystem::create_directories(database_path(_current_db), ec);
    if (ec) {
        return false;
    }
    TablePageManager manager(table_path(_current_db, stmt.name).string());
    if (!manager.write_schema(columns)) {
        return false;
    }
    tables.push_back(Table(stmt.name, std::move(columns)));
    return true;
}

bool Executor::execute_drop_table(const DropTableStmt& stmt)
{
    Database* db = find_current_database();
    if (db == nullptr) {
        return false;
    }
    auto& tables = db->tables();
    auto it = std::find_if(tables.begin(), tables.end(), [&](const Table& t){ return t.name() == stmt.name; });
    if (it == tables.end()) {
        return false;
    }
    std::error_code ec;
    std::filesystem::remove(table_path(_current_db, stmt.name), ec);
    if (ec) {
        return false;
    }
    for (const auto& col : it->columns()) {
        if (!col.indexed) {
            continue;
        }
        std::filesystem::remove(index_path(_current_db, stmt.name, col.name), ec);
        if (ec) {
            return false;
        }
    }
    tables.erase(it);
    return true;
}

bool Executor::normalize_insert_row(const Table& table,
                                    const InsertStmt& stmt,
                                    const std::vector<InsertValue>& raw_row,
                                    std::vector<int>& int_index_keys,
                                    std::vector<int>& bool_index_keys,
                                    std::vector<std::string>& string_index_keys,
                                    row_values_type& out_row) const
{
    if (stmt.columns.empty() || raw_row.size() != stmt.columns.size()) {
        return false;
    }
    const auto& table_columns = table.columns();
    std::unordered_map<std::string, std::size_t> table_column_pos;
    table_column_pos.reserve(table_columns.size());
    for (std::size_t i = 0; i < table_columns.size(); ++i) {
        table_column_pos[table_columns[i].name] = i;
    }

    out_row.assign(table_columns.size(), std::nullopt);
    int_index_keys.assign(table_columns.size(), 0);
    bool_index_keys.assign(table_columns.size(), 0);
    string_index_keys.assign(table_columns.size(), std::string());
    std::vector<bool> assigned(table_columns.size(), false);

    for (std::size_t i = 0; i < stmt.columns.size(); ++i) {
        const auto it_column = table_column_pos.find(stmt.columns[i]);
        if (it_column == table_column_pos.end()) {
            return false;
        }
        const std::size_t column_index = it_column->second;
        if (assigned[column_index]) {
            return false;
        }
        assigned[column_index] = true;
        const auto& column = table_columns[column_index];
        const auto& value = raw_row[i];
        if (value.is_null) {
            if (column.not_null || column.indexed) {
                return false;
            }
            out_row[column_index] = std::nullopt;
            continue;
        }
        if (column.type == "INT") {
            int parsed_int = 0;
            if (!parse_int_strict(value.text, parsed_int)) {
                return false;
            }
            out_row[column_index] = std::to_string(parsed_int);
            if (column.indexed) {
                int_index_keys[column_index] = parsed_int;
            }
            continue;
        }
        if (column.type == "STRING") {
            out_row[column_index] = value.text;
            if (column.indexed) {
                string_index_keys[column_index] = value.text;
            }
            continue;
        }
        if (column.type == "BOOL") {
            bool parsed_bool = false;
            if (!parse_bool_strict(value.text, parsed_bool)) {
                return false;
            }
            out_row[column_index] = parsed_bool ? "true" : "false";
            if (column.indexed) {
                bool_index_keys[column_index] = parsed_bool ? 1 : 0;
            }
            continue;
        }
        return false;
    }

    for (std::size_t i = 0; i < table_columns.size(); ++i) {
        if (assigned[i]) {
            continue;
        }
        if (table_columns[i].not_null || table_columns[i].indexed) {
            return false;
        }
        out_row[i] = std::nullopt;
    }
    return true;
}

bool Executor::execute_insert(const InsertStmt& stmt)
{
    Database* db = find_current_database();
    if (db == nullptr) return false;
    Table* table = find_table(*db, stmt.table_name);
    if (table == nullptr) return false;
    if (stmt.rows.empty() || stmt.columns.empty()) return false;

    struct PendingRow
    {
        row_values_type values;
        std::vector<int> int_index_keys;
        std::vector<int> bool_index_keys;
        std::vector<std::string> string_index_keys;
    };

    std::vector<PendingRow> pending_rows;
    pending_rows.reserve(stmt.rows.size());

    std::unordered_map<std::size_t, std::unordered_set<int>> seen_int_keys;
    std::unordered_map<std::size_t, std::unordered_set<int>> seen_bool_keys;
    std::unordered_map<std::size_t, std::unordered_set<std::string>> seen_string_keys;

    const auto& columns = table->columns();
    for (const auto& raw_row : stmt.rows) {
        PendingRow row{};
        if (!normalize_insert_row(*table, stmt, raw_row, row.int_index_keys, row.bool_index_keys, row.string_index_keys, row.values)) {
            return false;
        }

        for (std::size_t i = 0; i < columns.size(); ++i) {
            if (!columns[i].indexed) {
                continue;
            }
            if (!row.values[i].has_value()) {
                return false;
            }
            const std::filesystem::path idx_path = index_path(_current_db, table->name(), columns[i].name);
            if (columns[i].type == "INT") {
                const int key = row.int_index_keys[i];
                auto& seen = seen_int_keys[i];
                if (!seen.insert(key).second) {
                    return false;
                }
                IndexManager<int> index(idx_path.string());
                Rid existing{};
                if (index.find(key, existing)) {
                    return false;
                }
                continue;
            }
            if (columns[i].type == "STRING") {
                const std::string& value = row.string_index_keys[i];
                auto& seen = seen_string_keys[i];
                if (!seen.insert(value).second) {
                    return false;
                }
                StringIndexKey key{};
                if (!StringIndexKey::from_string(value, key)) {
                    return false;
                }
                IndexManager<StringIndexKey> index(idx_path.string());
                Rid existing{};
                if (index.find(key, existing)) {
                    return false;
                }
                continue;
            }
            if (columns[i].type == "BOOL") {
                const int key = row.bool_index_keys[i];
                auto& seen = seen_bool_keys[i];
                if (!seen.insert(key).second) {
                    return false;
                }
                IndexManager<int> index(idx_path.string());
                Rid existing{};
                if (index.find(key, existing)) {
                    return false;
                }
                continue;
            }
            return false;
        }

        pending_rows.push_back(std::move(row));
    }

    std::error_code ec;
    std::filesystem::create_directories(database_path(_current_db), ec);
    if (ec) {
        return false;
    }
    TablePageManager manager(table_path(_current_db, table->name()).string());
    for (const auto& row : pending_rows) {
        Record record(row.values);
        std::vector<unsigned char> bytes = serialize_record(record, columns.size());
        Page page;
        int slot_id = page.append_record(bytes.data(), static_cast<int>(bytes.size()));
        if (slot_id < 0) {
            return false;
        }
        int page_id = manager.allocate_page();
        if (!manager.write_page(page_id, page)) {
            return false;
        }
        const Rid rid{page_id, slot_id};
        for (std::size_t i = 0; i < columns.size(); ++i) {
            if (!columns[i].indexed) {
                continue;
            }
            const std::filesystem::path idx_path = index_path(_current_db, table->name(), columns[i].name);
            if (columns[i].type == "INT") {
                IndexManager<int> index(idx_path.string());
                if (!index.insert(row.int_index_keys[i], rid)) {
                    return false;
                }
                continue;
            }
            if (columns[i].type == "STRING") {
                StringIndexKey key{};
                if (!StringIndexKey::from_string(row.string_index_keys[i], key)) {
                    return false;
                }
                IndexManager<StringIndexKey> index(idx_path.string());
                if (!index.insert(key, rid)) {
                    return false;
                }
                continue;
            }
            if (columns[i].type == "BOOL") {
                IndexManager<int> index(idx_path.string());
                if (!index.insert(row.bool_index_keys[i], rid)) {
                    return false;
                }
                continue;
            }
            return false;
        }
    }

    return true;
}

void Executor::build_last_select_json()
{
    nlohmann::json arr = nlohmann::json::array();

    for (const auto& row : _last_select_rows) {
        nlohmann::json obj = nlohmann::json::object();

        for (std::size_t i = 0; i < _last_select_columns.size(); ++i) {
            const std::string& key = _last_select_columns[i];
            const std::optional<std::string> value =
                (i < row.size()) ? row[i] : std::nullopt;

            if (!value.has_value()) {
                obj[key] = nullptr;
                continue;
            }

            const std::string type =
                (i < _last_select_column_types.size()) ? _last_select_column_types[i] : "STRING";

            if (type == "INT") {
                int v = 0;
                if (parse_int_strict(*value, v)) obj[key] = v;
                else obj[key] = *value;
            } else if (type == "BOOL") {
                bool v = false;
                if (parse_bool_strict(*value, v)) obj[key] = v;
                else obj[key] = *value;
            } else {
                obj[key] = *value;
            }
        }

        arr.push_back(std::move(obj));
    }

    _last_select_json = arr.dump(2); 
}

bool Executor::execute_update(const UpdateStmt& stmt)
{
    Database* db = find_current_database();
    if (db == nullptr) return false;
    Table* table = find_table(*db, stmt.table_name);
    if (table == nullptr) return false;
    if (stmt.assignments.empty()) return false;
    const auto& columns = table->columns();

    std::unordered_map<std::string, std::size_t> col_pos;
    col_pos.reserve(columns.size());
    for (std::size_t i = 0; i < columns.size(); ++i) {
        col_pos[columns[i].name] = i;
    }

    std::vector<bool> has_assignment(columns.size(), false);
    std::vector<std::optional<std::string>> assignment_values(columns.size(), std::nullopt);
    std::vector<std::size_t> assigned_indexed_columns;
    assigned_indexed_columns.reserve(stmt.assignments.size());

    for (const auto& assignment : stmt.assignments) {
        const auto it = col_pos.find(assignment.column_name);
        if (it == col_pos.end()) return false;
        const std::size_t column_idx = it->second;
        if (has_assignment[column_idx]) return false;
        has_assignment[column_idx] = true;

        const Column& column = columns[column_idx];
        if (assignment.value.is_null) {
            if (column.not_null || column.indexed) return false;
            assignment_values[column_idx] = std::nullopt;
        } else if (column.type == "INT") {
            int parsed = 0;
            if (!parse_int_strict(assignment.value.text, parsed)) return false;
            assignment_values[column_idx] = std::to_string(parsed);
        } else if (column.type == "STRING") {
            assignment_values[column_idx] = assignment.value.text;
        } else if (column.type == "BOOL") {
            bool parsed = false;
            if (!parse_bool_strict(assignment.value.text, parsed)) return false;
            assignment_values[column_idx] = parsed ? "true" : "false";
        } else {
            return false;
        }

        if (column.indexed) {
            assigned_indexed_columns.push_back(column_idx);
        }
    }

    const WhereComparison* where_cmp = nullptr;
    if (stmt.where.has_value()) {
        where_cmp = std::get_if<WhereComparison>(&stmt.where.value());
        if (where_cmp == nullptr) return false;
    }

    struct PendingUpdate
    {
        Rid rid;
        row_values_type old_row;
        row_values_type new_row;
    };

    struct IndexChange
    {
        std::size_t column_idx;
        Rid rid;
        std::optional<std::string> old_value;
        std::optional<std::string> new_value;
    };

    TablePageManager manager(table_path(_current_db, table->name()).string());
    std::vector<PendingUpdate> pending;

    auto enqueue_row_if_matches = [&](const Rid& rid, row_values_type row) {
        if (where_cmp != nullptr && !evaluate_where_comparison(*where_cmp, row, columns, col_pos)) {
            return;
        }

        row_values_type updated = row;
        for (std::size_t i = 0; i < columns.size(); ++i) {
            if (has_assignment[i]) {
                updated[i] = assignment_values[i];
            }
        }
        if (updated == row) {
            return;
        }

        pending.push_back(PendingUpdate{rid, std::move(row), std::move(updated)});
    };

    bool used_index_candidates = false;
    if (where_cmp != nullptr) {
        IndexWherePlan index_plan{};
        if (try_build_index_where_plan(*where_cmp, columns, col_pos, index_plan)) {
            const Column& idx_col = columns[index_plan.column_idx];
            const std::filesystem::path idx_path = index_path(_current_db, table->name(), idx_col.name);
            bool used_index = false;
            std::vector<Rid> rid_candidates;
            if (!collect_rids_from_index(idx_path, idx_col, index_plan.op, index_plan.literal, used_index, rid_candidates)) {
                return false;
            }
            if (used_index) {
                used_index_candidates = true;
                pending.reserve(rid_candidates.size());
                for (const Rid& rid : rid_candidates) {
                    row_values_type row;
                    if (!read_row_by_rid(manager, rid, columns.size(), row)) {
                        continue;
                    }
                    enqueue_row_if_matches(rid, std::move(row));
                }
            }
        }
    }

    if (!used_index_candidates) {
        Page meta;
        if (!manager.read_page(0, meta)) return false;
        TableHeader th{};
        std::memcpy(&th, meta.data().data(), sizeof(TableHeader));

        for (int page_id = 1; page_id < th.next_page_id; ++page_id) {
            Page page;
            if (!manager.read_page(page_id, page)) continue;
            const PageHeader ph = page.read_header();
            for (int slot = 0; slot < ph.slots_count; ++slot) {
                auto raw = page.read_record(slot);
                if (raw.empty()) continue;
                Record rec = deserialize_record(raw.data(), columns.size());
                row_values_type row = rec.values();
                enqueue_row_if_matches(Rid{page_id, slot}, std::move(row));
            }
        }
    }

    if (pending.empty()) {
        return true;
    }

    for (std::size_t column_idx : assigned_indexed_columns) {
        const Column& column = columns[column_idx];
        const std::filesystem::path idx_path = index_path(_current_db, table->name(), column.name);

        if (column.type == "INT" || column.type == "BOOL") {
            IndexManager<int> index(idx_path.string());
            std::unordered_set<int> seen_keys;
            seen_keys.reserve(pending.size());
            for (const auto& update : pending) {
                const auto& old_value = update.old_row[column_idx];
                const auto& new_value = update.new_row[column_idx];
                if (old_value == new_value) continue;
                if (!new_value.has_value()) return false;

                int new_key = 0;
                if (column.type == "INT") {
                    if (!parse_int_strict(*new_value, new_key)) return false;
                } else {
                    bool parsed = false;
                    if (!parse_bool_strict(*new_value, parsed)) return false;
                    new_key = parsed ? 1 : 0;
                }

                if (!seen_keys.insert(new_key).second) return false;
                Rid existing{};
                if (index.find(new_key, existing) && existing != update.rid) {
                    return false;
                }
            }
            continue;
        }

        if (column.type == "STRING") {
            IndexManager<StringIndexKey> index(idx_path.string());
            std::unordered_set<std::string> seen_keys;
            seen_keys.reserve(pending.size());
            for (const auto& update : pending) {
                const auto& old_value = update.old_row[column_idx];
                const auto& new_value = update.new_row[column_idx];
                if (old_value == new_value) continue;
                if (!new_value.has_value()) return false;
                if (!seen_keys.insert(*new_value).second) return false;

                StringIndexKey key{};
                if (!StringIndexKey::from_string(*new_value, key)) return false;
                Rid existing{};
                if (index.find(key, existing) && existing != update.rid) {
                    return false;
                }
            }
            continue;
        }

        return false;
    }

    std::vector<IndexChange> index_changes;
    index_changes.reserve(pending.size() * assigned_indexed_columns.size());
    for (const auto& update : pending) {
        for (std::size_t column_idx : assigned_indexed_columns) {
            if (update.old_row[column_idx] == update.new_row[column_idx]) {
                continue;
            }
            index_changes.push_back(IndexChange{
                column_idx,
                update.rid,
                update.old_row[column_idx],
                update.new_row[column_idx]
            });
        }
    }

    std::unordered_map<int, Page> original_pages;
    std::unordered_map<int, Page> modified_pages;
    for (const auto& update : pending) {
        auto it_modified = modified_pages.find(update.rid.page_id);
        if (it_modified == modified_pages.end()) {
            Page page;
            if (!manager.read_page(update.rid.page_id, page)) return false;
            original_pages.emplace(update.rid.page_id, page);
            it_modified = modified_pages.emplace(update.rid.page_id, page).first;
        }

        Page& page = it_modified->second;
        PageHeader ph = page.read_header();
        if (update.rid.slot_id < 0 || update.rid.slot_id >= ph.slots_count) return false;
        Slot slot = page.read_slot(update.rid.slot_id);
        if (slot.size <= 0) return false;

        Record record(update.new_row);
        std::vector<unsigned char> bytes = serialize_record(record, columns.size());
        const int record_size = static_cast<int>(bytes.size());

        if (record_size <= slot.size) {
            std::memcpy(page.data().data() + slot.offset, bytes.data(), bytes.size());
            slot.size = record_size;
            page.write_slot(update.rid.slot_id, slot);
            continue;
        }

        const int slot_dir_end = static_cast<int>(sizeof(PageHeader) + ph.slots_count * sizeof(Slot));
        if (ph.free_end - slot_dir_end < record_size) return false;
        ph.free_end -= record_size;
        std::memcpy(page.data().data() + ph.free_end, bytes.data(), bytes.size());
        slot.offset = ph.free_end;
        slot.size = record_size;
        page.write_slot(update.rid.slot_id, slot);
        page.write_header(ph);
    }

    auto apply_index_change = [&](const IndexChange& change) -> bool {
        const Column& column = columns[change.column_idx];
        const std::filesystem::path idx_path = index_path(_current_db, table->name(), column.name);
        if (!change.old_value.has_value() || !change.new_value.has_value()) {
            return false;
        }

        if (column.type == "INT") {
            int old_key = 0;
            int new_key = 0;
            if (!parse_int_strict(*change.old_value, old_key) || !parse_int_strict(*change.new_value, new_key)) return false;
            IndexManager<int> index(idx_path.string());
            return index.erase(old_key) && index.insert(new_key, change.rid);
        }

        if (column.type == "BOOL") {
            bool old_bool = false;
            bool new_bool = false;
            if (!parse_bool_strict(*change.old_value, old_bool) || !parse_bool_strict(*change.new_value, new_bool)) return false;
            IndexManager<int> index(idx_path.string());
            return index.erase(old_bool ? 1 : 0) && index.insert(new_bool ? 1 : 0, change.rid);
        }

        if (column.type == "STRING") {
            StringIndexKey old_key{};
            StringIndexKey new_key{};
            if (!StringIndexKey::from_string(*change.old_value, old_key)) return false;
            if (!StringIndexKey::from_string(*change.new_value, new_key)) return false;
            IndexManager<StringIndexKey> index(idx_path.string());
            return index.erase(old_key) && index.insert(new_key, change.rid);
        }

        return false;
    };

    auto rollback_index_change = [&](const IndexChange& change) -> bool {
        const Column& column = columns[change.column_idx];
        const std::filesystem::path idx_path = index_path(_current_db, table->name(), column.name);
        if (!change.old_value.has_value() || !change.new_value.has_value()) {
            return false;
        }

        if (column.type == "INT") {
            int old_key = 0;
            int new_key = 0;
            if (!parse_int_strict(*change.old_value, old_key) || !parse_int_strict(*change.new_value, new_key)) return false;
            IndexManager<int> index(idx_path.string());
            return index.erase(new_key) && index.insert(old_key, change.rid);
        }

        if (column.type == "BOOL") {
            bool old_bool = false;
            bool new_bool = false;
            if (!parse_bool_strict(*change.old_value, old_bool) || !parse_bool_strict(*change.new_value, new_bool)) return false;
            IndexManager<int> index(idx_path.string());
            return index.erase(new_bool ? 1 : 0) && index.insert(old_bool ? 1 : 0, change.rid);
        }

        if (column.type == "STRING") {
            StringIndexKey old_key{};
            StringIndexKey new_key{};
            if (!StringIndexKey::from_string(*change.old_value, old_key)) return false;
            if (!StringIndexKey::from_string(*change.new_value, new_key)) return false;
            IndexManager<StringIndexKey> index(idx_path.string());
            return index.erase(new_key) && index.insert(old_key, change.rid);
        }

        return false;
    };

    std::size_t applied_changes = 0;
    for (const auto& change : index_changes) {
        if (!apply_index_change(change)) {
            while (applied_changes > 0) {
                --applied_changes;
                rollback_index_change(index_changes[applied_changes]);
            }
            return false;
        }
        ++applied_changes;
    }

    std::vector<int> written_pages;
    written_pages.reserve(modified_pages.size());
    for (const auto& [page_id, page] : modified_pages) {
        if (!manager.write_page(page_id, page)) {
            for (int written_page_id : written_pages) {
                const auto it_original = original_pages.find(written_page_id);
                if (it_original != original_pages.end()) {
                    manager.write_page(written_page_id, it_original->second);
                }
            }
            while (applied_changes > 0) {
                --applied_changes;
                rollback_index_change(index_changes[applied_changes]);
            }
            return false;
        }
        written_pages.push_back(page_id);
    }

    return true;
}


bool Executor::execute_select(const SelectStmt& stmt){
    _last_select_columns.clear();
    _last_select_rows.clear();
    _last_select_column_types.clear();
    _last_select_json.clear();
    Database* db = find_current_database();
    if (db == nullptr) return false;
    Table* table = find_table(*db, stmt.table_name);
    if (table == nullptr) return false;
    const auto& columns = table->columns();
    std::unordered_map<std::string, std::size_t> col_pos;
    for (std::size_t i = 0; i < columns.size(); ++i) col_pos[columns[i].name] = i;
    std::vector<std::size_t> selected_idx;
    if (stmt.projection.is_star) {
        for (std::size_t i = 0; i < columns.size(); ++i) {
            selected_idx.push_back(i);
            _last_select_columns.push_back(columns[i].name);
            _last_select_column_types.push_back(columns[i].type);
        }
    } else {
        for (const auto& item : stmt.projection.items) {
            auto it = col_pos.find(item.column_name);
            if (it == col_pos.end()) return false;
            const std::size_t column_idx = it->second;
            selected_idx.push_back(column_idx);
            _last_select_columns.push_back(item.alias.has_value() ? item.alias.value() : item.column_name);
            _last_select_column_types.push_back(columns[column_idx].type);
        }
    }
    const WhereComparison* where_cmp = nullptr;
    if (stmt.where.has_value()) {
        where_cmp = std::get_if<WhereComparison>(&stmt.where.value());
        if (where_cmp == nullptr) return false; // BETWEEN/LIKE позже
    }
    TablePageManager manager(table_path(_current_db, table->name()).string());

    if (where_cmp != nullptr) {
        IndexWherePlan index_plan{};
        if (try_build_index_where_plan(*where_cmp, columns, col_pos, index_plan)) {
            const Column& idx_col = columns[index_plan.column_idx];
            const std::filesystem::path idx_path = index_path(_current_db, table->name(), idx_col.name);
            bool used_index = false;
            std::vector<Rid> rid_candidates;
            if (!collect_rids_from_index(idx_path, idx_col, index_plan.op, index_plan.literal, used_index, rid_candidates)) {
                return false;
            }
            if (used_index) {
                for (const Rid& rid : rid_candidates) {
                    row_values_type row;
                    if (!read_row_by_rid(manager, rid, columns.size(), row)) {
                        continue;
                    }

                    bool pass = true;
                    if (where_cmp != nullptr) {
                        pass = evaluate_where_comparison(*where_cmp, row, columns, col_pos);
                    }
                    if (!pass) continue;

                    row_values_type out;
                    out.reserve(selected_idx.size());
                    for (std::size_t idx : selected_idx) out.push_back(row[idx]);
                    _last_select_rows.push_back(std::move(out));
                }

                build_last_select_json();
                return true;
            }
        }
    }

    Page meta;
    if (!manager.read_page(0, meta)) return false;
    TableHeader th{};
    std::memcpy(&th, meta.data().data(), sizeof(TableHeader));

    for (int page_id = 1; page_id < th.next_page_id; ++page_id) {
        Page page;
        if (!manager.read_page(page_id, page)) continue;
        PageHeader ph = page.read_header();
        for (int slot = 0; slot < ph.slots_count; ++slot) {
            auto raw = page.read_record(slot);
            if (raw.empty()) continue;

            Record rec = deserialize_record(raw.data(), columns.size());
            row_values_type row = rec.values();

            // TODO: apply where_cmp (comparison only)
            bool pass = true;
            if (where_cmp != nullptr) {
                pass = evaluate_where_comparison(*where_cmp, row, columns, col_pos); 
            }
            if (!pass) continue;

            row_values_type out;
            out.reserve(selected_idx.size());
            for (std::size_t idx : selected_idx) out.push_back(row[idx]);
            _last_select_rows.push_back(std::move(out));
        }
    }
    build_last_select_json();
    return true;
    }
}
