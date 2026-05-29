# Coursework DBMS

Учебная файловая СУБД для курсовой работы по системному программированию.
Проект реализует базовый пункт 0 из ТЗ: базы данных, таблицы, типизированные
записи, SQL-подобный язык, B-tree индексы, файловую персистентность,
интерактивный и пакетный CLI. Дополнительно есть базовая gRPC
клиент-серверная обвязка.

## Возможности

- Иерархия хранения: system -> databases -> tables.
- Обязательные по ТЗ типы колонок: `INT`, `STRING`.
- В коде есть экспериментальная поддержка `BOOL`, но базовая проверяемая
  часть проекта опирается на `INT` и `STRING`.
- Ограничения колонок:
  - nullable по умолчанию;
  - `NOT NULL`;
  - `INDEXED`: уникальное поле, не может быть `NULL`, индекс создается автоматически.
- DDL:
  - `CREATE DATABASE name;`
  - `DROP DATABASE name;`
  - `USE name;`
  - `CREATE TABLE name (...);`
  - `DROP TABLE name;`
- DML:
  - `INSERT INTO table (cols...) VALUE (...), (...);`
  - `UPDATE table SET col = value, ... WHERE condition;`
  - `DELETE FROM table WHERE condition;`
  - `SELECT * FROM table WHERE condition;`
  - `SELECT col AS alias, ... FROM table WHERE condition;`
- Обращение к таблицам через активную БД (`USE`) или полное имя
  `database.table` для `INSERT`, `SELECT`, `UPDATE`, `DELETE`.
- Условия `WHERE`:
  - сравнения `==`, `!=`, `<`, `>`, `<=`, `>=`;
  - `BETWEEN`, закрытый интервал `[left, right]`;
  - `LIKE` с регулярным выражением.
- Результат успешного `SELECT` печатается как JSON-массив объектов.
- Команды могут занимать несколько строк и завершаются `;`.
- Ключевые слова регистронезависимы, но смешанный регистр внутри одного
  ключевого слова запрещен.

## Сборка

```bash
cmake -S . -B build
cmake --build build -j 4
```

Основной исполняемый файл:

```text
build/prog
```

Тестовые бинарники находятся в:

```text
build/tests/bin
```

## Запуск CLI

Интерактивный режим:

```bash
./build/prog
```

Пакетный режим:

```bash
./build/prog script.sql
```

По умолчанию данные лежат во временной директории `coursework_dbms_data`.
Чтобы явно выбрать место хранения:

```bash
export DBMS_DATA_ROOT=/tmp/coursework-dbms-demo
./build/prog
```

Пример сессии:

```sql
CREATE DATABASE demo;
USE demo;
CREATE TABLE users (id INT INDEXED, name STRING, age INT NOT NULL);
INSERT INTO users (id, name, age) VALUE
  (1, "alice", 20),
  (2, "bob", 25),
  (3, NULL, 30);
SELECT id AS user_id, name FROM users WHERE id >= 2;
```

Пример JSON-результата:

```json
[
  {
    "user_id": 2,
    "name": "bob"
  },
  {
    "user_id": 3,
    "name": null
  }
]
```

## gRPC

Сервер:

```bash
./build/dbms_grpc_server 0.0.0.0:50051 /tmp/coursework-dbms-demo
```

Если `data_root` не передан, сервер использует тот же путь по умолчанию, что и
CLI: `DBMS_DATA_ROOT` или временную директорию.

Интерактивный клиент:

```bash
./build/dbms_grpc_client 127.0.0.1:50051
```

Клиент со скриптом:

```bash
./build/dbms_grpc_client 127.0.0.1:50051 /path/to/script.sql
```

gRPC API состоит из трех вызовов:

- `OpenSession`
- `Execute`
- `CloseSession`

Текущий интерактивный gRPC client проще основного CLI: он рассчитан на ввод
одной SQL-команды за раз. Пакетный gRPC client умеет разделять команды по `;`.

## Хранение на диске

Структура файлов:

```text
data_root/
  database_name/
    table_name.tbl
    table_name__indexed_column.idx
```

`.tbl`:

- page `0` хранит `TableHeader` и сериализованную схему колонок;
- записи лежат на страницах `1..next_page_id-1`;
- физический адрес записи представлен как `Rid{page_id, slot_id}`;
- удаление логическое: slot помечается пустым, уплотнение страницы не делается.

`.idx`:

- хранит страницы B-tree;
- ключ индекса - значение indexed-колонки;
- значение индекса - `Rid` записи в `.tbl`;
- сами записи таблицы в индекс не копируются.

## Индексы

Индексы создаются автоматически для колонок с `INDEXED`.
Для таких колонок:

- `NULL` запрещен;
- значения уникальны;
- `INSERT`, `UPDATE`, `DELETE` поддерживают согласованность `.tbl` и `.idx`;
- при ошибке обновления одного индекса уже примененные индексные изменения
  откатываются.

Оптимизированный путь через индекс используется для простых условий вида:

```sql
WHERE indexed_column == literal
WHERE indexed_column <  literal
WHERE indexed_column <= literal
WHERE indexed_column >  literal
WHERE indexed_column >= literal
WHERE indexed_column BETWEEN left_literal AND right_literal
```

Также поддерживается вариант с литералом слева:

```sql
WHERE 10 <= indexed_column
```

Для `BETWEEN` используется закрытый интервал `[left_literal, right_literal]`.

Для `!=`, неиндексируемых колонок, column-vs-column сравнений и `LIKE`
используется full scan.

## Надежность

Что гарантируется штатно:

- данные, схемы и индексы сохраняются в файловой системе;
- каталог БД и таблиц восстанавливается при новом запуске `Dbms` на том же
  `data_root`;
- после успешных штатных `INSERT`, `UPDATE`, `DELETE` таблица и индексы
  согласованы;
- поврежденный или отсутствующий `.idx`, обрезанный `.tbl` и битый `Rid`
  приводят к runtime error, а не к молча неверному результату;
- `CREATE TABLE` откатывает уже созданные файлы, если один из индексов не
  удалось создать;
- `DROP TABLE` предварительно проверяет table/index paths и не удаляет `.tbl`,
  если индексный путь уже поврежден.

Честные ограничения:

- нет WAL, journal и copy-on-write;
- нет crash recovery после `kill -9` или отключения питания между page writes;
- нет ACID-транзакций между несколькими SQL-командами;
- нет поддержки конкурентных писателей;
- нет фоновой перестройки индексов из таблицы.

## Тесты

Полный прогон:

```bash
cmake --build build --target run_all_tests -j 4
```

Этот target собирает и запускает:

- B-tree tests;
- lexer/parser/executor tests;
- CLI script tests;
- spec-style tests по пункту 0 ТЗ.

## Demo Scripts

Готовые скрипты для защиты лежат в `scripts/`.

Happy path по пункту 0:

```bash
rm -rf /tmp/coursework-demo-point0
DBMS_DATA_ROOT=/tmp/coursework-demo-point0 ./build/prog scripts/demo_point0.sql
```

Constraints и информативные ошибки:

```bash
rm -rf /tmp/coursework-demo-errors
DBMS_DATA_ROOT=/tmp/coursework-demo-errors ./build/prog scripts/demo_constraints_errors.sql
```

Этот скрипт специально завершается с ненулевым кодом, потому что внутри есть
ожидаемые ошибки. CLI продолжает выполнение и показывает финальный `SELECT`.

Restart persistence:

```bash
rm -rf /tmp/coursework-demo-restart
DBMS_DATA_ROOT=/tmp/coursework-demo-restart ./build/prog scripts/demo_restart_seed.sql
DBMS_DATA_ROOT=/tmp/coursework-demo-restart ./build/prog scripts/demo_restart_check.sql
```

gRPC script mode:

```bash
rm -rf /tmp/coursework-demo-grpc
./build/dbms_grpc_server 127.0.0.1:50051 /tmp/coursework-demo-grpc
./build/dbms_grpc_client 127.0.0.1:50051 scripts/demo_grpc.sql
```

## Полезные файлы

- `src/sql/lexer.cpp` - лексер SQL-подобного языка.
- `src/sql/parser.cpp` - parser и AST statements.
- `src/sql/executor.cpp` - семантика DDL/DML, constraints, JSON output,
  работа с индексами.
- `include/dbms/storage/` - страницы таблиц, records, serialization.
- `include/dbms/index/` - B-tree disk index и page manager.
- `src/sql/cli.cpp` - интерактивный и пакетный CLI.
- `src/grpc/` и `proto/sql_service.proto` - gRPC server/client.
