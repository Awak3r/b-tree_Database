# Описание работы SQL-операторов

Каждый оператор проходит общий путь:

```
Строка SQL
    │
    ▼
Lexer → список Token
    │
    ▼
Parser → Statement (AST)
    │
    ▼
Executor::execute(stmt)
    │
    ▼
execute_<имя>(конкретный stmt)
```

---

## 1. CREATE DATABASE

```sql
CREATE DATABASE demo;
```

### AST
```
CreateDatabaseStmt { name: "demo" }
```

### Поток выполнения
```
execute_create_database()
    │
    ├─ ищем "demo" в catalog.databases()
    │       найден? → fail "already exists"
    │
    ├─ create_directories(data_root / "demo")
    │       ошибка? → fail "cannot create"
    │
    └─ catalog.databases().push_back(Database("demo"))
```

### Результат на диске
```
data_root/
  demo/          ← создана пустая директория
```

---

## 2. DROP DATABASE

```sql
DROP DATABASE demo;
```

### AST
```
DropDatabaseStmt { name: "demo" }
```

### Поток выполнения
```
execute_drop_database()
    │
    ├─ ищем "demo" в catalog.databases()
    │       не найден? → fail "does not exist"
    │
    ├─ remove_all(data_root / "demo")
    │       ошибка? → fail "cannot drop"
    │
    ├─ catalog.databases().erase(it)
    │
    └─ если _current_db == "demo" → _current_db = ""
```

### Что удаляется
```
data_root/
  demo/          ← директория со всем содержимым удалена рекурсивно
    users.tbl
    users__id.idx
```

---

## 3. USE

```sql
USE demo;
```

### AST
```
UseDatabaseStmt { name: "demo" }
```

### Поток выполнения
```
execute_use_database()
    │
    ├─ ищем "demo" в catalog.databases()
    │       не найден? → fail "does not exist"
    │
    └─ _current_db = "demo"
```

Никаких файловых операций. Просто устанавливает контекст для
последующих команд без явного указания db.table.

---

## 4. CREATE TABLE

```sql
CREATE TABLE users (
    id   INT    INDEXED,
    name STRING NOT NULL,
    age  INT    DEFAULT 0
);
```

### AST
```
CreateTableStmt {
    name: "users",
    columns: [
        ColumnDef { name:"id",   type:"INT",    indexed:true,  not_null:false, default:nullopt },
        ColumnDef { name:"name", type:"STRING", indexed:false, not_null:true,  default:nullopt },
        ColumnDef { name:"age",  type:"INT",    indexed:false, not_null:false, default:"0"     }
    ]
}
```

### Поток выполнения
```
execute_create_table()
    │
    ├─ find_current_database()
    │       нет активной БД? → fail
    │
    ├─ найти "users" среди таблиц → уже есть? → fail "already exists"
    │
    ├─ валидация колонок:
    │     для каждой колонки:
    │       is_type_valid(type)?  → INT/STRING/BOOL, иначе fail
    │       дублирование имён?    → fail "duplicate column"
    │
    ├─ create_directories(data_root / db_name)
    │
    ├─ TablePageManager manager("demo/users.tbl")
    │   manager.write_schema(columns)
    │       → сериализует схему в Page 0
    │
    ├─ для каждой INDEXED колонки:
    │     idx_path = "demo/users__id.idx"
    │     IndexManager<int> index(idx_path)   ← создаёт файл с пустым B-tree
    │     файл не создался? → откат:
    │         удалить ранее созданные .idx
    │         удалить .tbl
    │         → fail
    │     created_index_paths.push_back(idx_path)
    │
    └─ db.tables().push_back(Table("users", columns))
```

### Результат на диске
```
data_root/demo/
    users.tbl              ← Page 0: TableHeader + схема колонок
    users__id.idx          ← Page 0: пустой IndexHeader (B-tree пуст)
```

---

## 5. DROP TABLE

```sql
DROP TABLE users;
```

### AST
```
DropTableStmt { name: "users" }
```

### Поток выполнения
```
execute_drop_table()
    │
    ├─ find_current_database() → нет? → fail
    │
    ├─ найти "users" → не найдена? → fail "does not exist"
    │
    ├─ ── ПРЕДПОЛЁТНАЯ ПРОВЕРКА (preflight) ──
    │   tbl_path существует и является файлом?
    │       нет → fail "missing or not a regular file"
    │
    │   для каждой INDEXED колонки:
    │       idx_path существует, но НЕ является файлом?
    │           → fail "not a regular file"
    │       (если не существует вообще — пропускаем)
    │
    ├─ remove(tbl_path)
    │       ошибка → fail
    │
    ├─ для каждой INDEXED колонки:
    │     remove(idx_path)
    │       ошибка → fail
    │
    └─ db.tables().erase(it)
```

Preflight нужен чтобы не удалять .tbl если .idx повреждён —
оставляем систему в согласованном состоянии.

---

## 6. INSERT

```sql
INSERT INTO users (id, name, age) VALUE
    (1, "alice", 20),
    (2, "bob",   25);
```

### AST
```
InsertStmt {
    table_name: "users",
    columns: ["id", "name", "age"],
    rows: [
        [ InsertValue{false,"1"}, InsertValue{false,"alice"}, InsertValue{false,"20"} ],
        [ InsertValue{false,"2"}, InsertValue{false,"bob"},   InsertValue{false,"25"} ]
    ]
}
```

### Поток выполнения
```
execute_insert()
    │
    ├─ resolve + find db + find table
    │
    ├─ ── ФАЗА 1: валидация всего батча ──
    │   для каждой строки:
    │     normalize_insert_row():
    │       проверить кол-во значений == кол-ву колонок
    │       для каждого значения:
    │         NULL в NOT NULL / INDEXED колонке? → fail
    │         INT: parse_int_strict()             → fail если не число
    │         STRING: взять текст как есть
    │         сохранить index_key если колонка INDEXED
    │
    │     для каждой INDEXED колонки строки:
    │       дубликат в текущем батче?    → fail (seen_keys)
    │       IndexManager::find(key)?     → fail "duplicate INDEXED key"
    │
    │   Если хоть одна строка не прошла — выходим, ничего не пишем
    │
    ├─ ── ФАЗА 2: запись на диск ──
    │   читаем оригинальный Page 0 (TableHeader)
    │
    │   для каждой pending_row:
    │     serialize_record(row) → bytes
    │     manager.write_page(page_id, page)  ← данные
    │     Rid = { page_id, slot_id }
    │
    │   для каждой INDEXED колонки:
    │     IndexManager::insert(key, rid)
    │     ошибка?
    │       → откатить уже вставленные индексы
    │       → restore original_header_page
    │       → fail
    │
    └─ writer обновляет TableHeader (next_page_id)
```

### Ключевой момент: откат индексов
```
Строки 1 и 2 вставлены в .tbl
Индекс по "id" для строки 1 записан
Индекс по "id" для строки 2 — ОШИБКА
    │
    ├─ rollback: index.erase(key строки 1)
    ├─ restore: записать original_header_page обратно в Page 0
    └─ fail
```

---

## 7. SELECT

```sql
SELECT id AS uid, name FROM users WHERE id >= 2;
```

### AST
```
SelectStmt {
    projection: SelectProjection {
        is_star: false,
        items: [
            SelectItem { column_name:"id",   alias:"uid",    aggregate:nullopt },
            SelectItem { column_name:"name", alias:nullopt,  aggregate:nullopt }
        ]
    },
    table_name: "users",
    where: WhereComparison {
        lhs: ColumnRef{"id"}, op: ge, rhs: Literal{"2"}
    }
}
```

### Поток выполнения
```
execute_select()
    │
    ├─ resolve + find db + find table
    │
    ├─ строим col_pos: { "id"→0, "name"→1, "age"→2 }
    │
    ├─ has_aggregates? → нет
    │
    ├─ selected_idx = [0, 1]  (индексы id и name)
    │   _last_select_columns = ["uid", "name"]
    │
    ├─ collect_matching_rows()  ← см. ниже
    │
    ├─ проекция: для каждой matched_row взять [row[0], row[1]]
    │
    └─ build_last_select_json()
            INT колонка  → число в JSON
            STRING       → строка или null
```

### collect_matching_rows — выбор пути
```
WHERE id >= 2
    │
    ├─ validate_where_condition() → типы и имена колонок
    │
    ├─ Это WhereComparison?       → ДА
    │   try_build_index_where_plan()
    │       "id" — INDEXED INT?   → ДА
    │           ↓
    │     ИНДЕКСНЫЙ ПУТЬ
    │     collect_rids_from_index(idx_path, ge, "2")
    │         BTreeDiskIndex::range(2, MAX_INT)
    │         → [ Rid{1,1}, Rid{1,2} ]
    │     для каждого RID:
    │         read_row_by_rid()
    │         evaluate_where_condition() ← перепроверка
    │         → matched_rows
    │
    ├─ Это WhereBetween? → аналогично через range()
    │
    └─ Индекс не подошёл (!=, LIKE, AND/OR, col==col)?
            FULL SCAN
            читаем TableHeader из Page 0 → next_page_id
            for page_id in 1..N:
                читаем Page
                for slot in 0..slots_count:
                    read_record(slot)
                    deserialize_record()
                    evaluate_where_condition()
                    → matched_rows
```

### SELECT с агрегатами
```sql
SELECT COUNT(id), AVG(age) FROM users WHERE age > 18;
```
```
has_aggregates = true
    │
    ├─ collect_matching_rows() → matched_rows (те же пути)
    │
    └─ для каждого SelectItem с aggregate:
            COUNT: считаем ненулл строки           → "3"
            SUM:   складываем значения             → "90"
            AVG:   sum/count, fixed precision 2    → "30.00"
                   пустой результат                → null

            результат: ОДНА строка в _last_select_rows
```

---

## 8. UPDATE

```sql
UPDATE users SET age = 30, name = "carol" WHERE id == 1;
```

### AST
```
UpdateStmt {
    table_name: "users",
    assignments: [
        UpdateAssignment { column_name:"age",  value:{false,"30"}    },
        UpdateAssignment { column_name:"name", value:{false,"carol"} }
    ],
    where: WhereComparison { lhs:ColumnRef{"id"}, op:eq, rhs:Literal{"1"} }
}
```

### Поток выполнения
```
execute_update()
    │
    ├─ resolve + find db + find table
    │
    ├─ ── валидация assignments ──
    │   для каждого assignment:
    │     колонка существует?       → иначе fail
    │     дублирование колонки?     → fail
    │     NULL в NOT NULL/INDEXED?  → fail
    │     тип совместим?            → parse_int / parse_bool
    │     INDEXED? → добавить в assigned_indexed_columns
    │
    ├─ collect_matching_rows()  ← те же индексный/full scan пути
    │
    ├─ строим pending:
    │   для каждой matched_row:
    │     new_row = old_row с применёнными assignments
    │     new_row == old_row? → пропускаем (нет изменений)
    │
    ├─ pending пуст? → return true (ничего не менять)
    │
    ├─ ── для каждой INDEXED колонки в assignments ──
    │   проверяем уникальность новых ключей:
    │     дубликат в батче?              → fail
    │     find(new_key) в индексе?       → fail "duplicate"
    │
    │   apply_index_mutations_with_rollback():
    │     для каждой pending_row:
    │         IndexMutation { old_value → new_value }
    │         index.erase(old_key)
    │         index.insert(new_key, rid)
    │         ошибка? → откатить все уже применённые
    │
    ├─ write_modified_pages_with_rollback():
    │   для каждой pending_row:
    │     serialize new_row → bytes
    │     записать в тот же slot (page_id, slot_id)
    │     ошибка?
    │         → rollback всех индексных изменений
    │         → restore original_pages
    │
    └─ return true
```

---

## 9. DELETE

```sql
DELETE FROM users WHERE id == 2;
```

### AST
```
DeleteStmt {
    table_name: "users",
    where: WhereComparison { lhs:ColumnRef{"id"}, op:eq, rhs:Literal{"2"} }
}
```

### Поток выполнения
```
execute_delete()
    │
    ├─ resolve + find db + find table
    │
    ├─ collect_matching_rows()  ← индексный или full scan
    │
    ├─ matched_rows пуст? → return true
    │
    ├─ ── строим index_changes ──
    │   для каждой matched_row, для каждой INDEXED колонки:
    │     IndexMutation { old_value → nullopt }
    │
    ├─ apply_index_mutations_with_rollback(index_changes)
    │   index.erase(old_key)
    │   ошибка? → откатить уже применённые → fail
    │
    ├─ для каждой matched_row:
    │     ensure_page_in_batch() ← читаем страницу, кэшируем original
    │     page.remove_record(slot_id)
    │         → Slot.size = 0  (логическое удаление, слот остаётся)
    │     ошибка?
    │         → rollback_applied_index_mutations()
    │         → fail
    │
    └─ write_modified_pages_with_rollback()
            записать изменённые страницы
            ошибка?
                → rollback индексов
                → restore original_pages
```

### Важно: удаление логическое
```
До:
  Page 1:
    slot 0: [ id=1, name="alice", age=20 ]
    slot 1: [ id=2, name="bob",   age=25 ]  ← удаляем
    slot 2: [ id=3, name="carol", age=30 ]

После:
  Page 1:
    slot 0: [ id=1, name="alice", age=20 ]
    slot 1: size=0  ← пустой слот, место не освобождается
    slot 2: [ id=3, name="carol", age=30 ]
```
Уплотнения страниц нет — слот просто помечается пустым.

---

## Общие паттерны

### resolve_table_name
```
"users"        → _current_db + "users"   (нужен USE)
"demo.users"   → db="demo", table="users" (явное указание)
нет точки и нет _current_db → fail
```

### apply_index_mutations_with_rollback
```
mutations = [
    { col_idx, rid, old_value, new_value }
]

for i in 0..mutations.size():
    если old_value.has_value() → index.erase(old_value)
    если new_value.has_value() → index.insert(new_value, rid)
    ошибка:
        for j in i-1..0:   ← откат в обратном порядке
            invert(mutations[j]) и применить
        fail

applied_count++ на каждом успешном шаге
```

### write_modified_pages_with_rollback
```
original_pages: { page_id → Page до изменений }
modified_pages: { page_id → Page после изменений }

for each modified page:
    write_page(page_id, modified)
    ошибка:
        rollback_callback()      ← откат индексов
        for each уже записанный: ← откат страниц
            write_page(page_id, original)
        fail
```
