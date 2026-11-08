# 数据库内省

PostgreSQL 后端提供完整的数据库内省功能，使用 `pg_catalog` 系统表查询数据库结构元数据。

## 概述

PostgreSQL 内省系统位于 `backend.introspector` 属性中，提供：

- **数据库信息**：名称、版本、编码、数据库大小
- **表信息**：表列表、表详情（包括物化视图、分区表、外部表）
- **列信息**：列名、数据类型、是否可空、默认值
- **索引信息**：索引名、列、唯一性、索引类型（BTREE、GIN、GiST 等）
- **外键信息**：引用表、列映射、更新/删除行为
- **视图信息**：视图定义 SQL
- **触发器信息**：触发事件、执行时机
- **运维测量**：未使用索引、表与索引的使用计数、实时运行指标、依赖关系与 DDL
- **列数据剖析**：来自 `pg_stats` 的逐列统计、保留值样本与连接覆盖率

## PostgreSQL 回答什么

核心声明了二十一个 `supports_*` 探针，每个探针问的是关于引擎的问题，而不是关于库的功能开关。PostgreSQL 其中十九个回答 `True`。下表是本后端对每一个探针的回答，顺序与核心声明的一致，并列出每个答案所读取的目录视图或语句。

| 探针 | 回答 | PostgreSQL 的答案来自 |
|------|------|----------------------|
| `supports_introspection` | `True` | 本方言自身的声明；七个内省作用域全部声明 |
| `supports_database_info` | `True` | `pg_database`、`version()`、`pg_database_size()` |
| `supports_table_introspection` | `True` | `pg_class` / `pg_namespace`，左连接 `pg_stat_user_tables` 取得活跃行数 |
| `supports_column_introspection` | `True` | `pg_attribute`、`pg_attrdef`、`pg_index`、`information_schema.columns` |
| `supports_index_introspection` | `True` | `pg_index`、`pg_class`、`pg_am` |
| `supports_foreign_key_introspection` | `True` | `pg_constraint`（`contype = 'f'`） |
| `supports_view_introspection` | `True` | `pg_class`（`relkind = 'v'`）、`pg_get_viewdef()` |
| `supports_trigger_introspection` | `True` | `pg_trigger`、`pg_class`、`pg_proc` |
| `supports_runtime_stats` | `True` | `pg_stat_activity` |
| `supports_index_stats` | `True` | `pg_stat_user_indexes`、`pg_relation_size()` |
| `supports_table_stats` | `True` | `pg_stat_user_tables` |
| `supports_unused_indexes_detection` | `True` | `pg_stat_user_indexes.idx_scan` |
| `supports_partition_info` | `False` | 继承自核心的默认值；没有分区信息格式化方法（见下） |
| `supports_object_dependencies` | `True` | `pg_depend`、`pg_class`、`pg_namespace` |
| `supports_column_data_profile` | `True` | `pg_stats` |
| `supports_column_value_sample` | `True` | `pg_stats.most_common_vals` |
| `supports_join_coverage` | `True` | 统计两侧的不同值；没有目录来源 |
| `supports_ddl_extraction` | `True` | `pg_class` / `pg_namespace`；投影不携带 DDL 文本 |
| `supports_extensions` | `True` | `pg_extension`，经由 `supports_extension_introspection` |
| `supports_table_storage_introspection` | `True` | `pg_total_relation_size()`、`pg_relation_size()`、`pg_indexes_size()` |
| `supports_ddl_extraction_native` | `False` | 没有单一的原生 DDL 获取调用（见下） |

有两个回答是 `False`。一个原因写在探针自己身上；另一个继承自核心的默认值，默认值没有写出的部分在下文说明。

### 两个 False 回答

`supports_partition_info` 通过核心的默认值回答 `False`：本方言不覆盖该探针，也没有声明任何分区信息查询表达式或格式化方法。分区表仍然会被回答——表列表会用 `table_type` 标签 `PARTITIONED TABLE` 报告它，来自 `p` 这个 relkind 代码——但那是关系的种类，不是分区树：这里没有任何东西报告父表、子表或边界。分区 DDL 是另一个协议族（`supports_hash_partitioning`、`supports_attach_partition` 及其邻居），不是这个内省探针。

`supports_ddl_extraction_native` 回答 `False`。探针自己的文档字符串就是原因：PostgreSQL 没有单一的、原生的 DDL 获取调用。`pg_get_expr` 与 `pg_get_viewdef` 能恢复表达式，但一个忠实的 `CREATE TABLE` 还需要存储参数、表空间以及目录只分开保存的逐列子句，所以本后端能提供的只是一次组装，而 `native=False` 正是组装结果的标记方式。`supports_ddl_extraction` 仍为 `True`，因为该族已被声明且有为它渲染的语句；语句本身在下文 `get_ddl_statement` 处描述。

### PostgreSQL 额外的探针

还有三个探针属于本后端而非核心协议，且都回答 `True`：`supports_extension_introspection` 读取 `pg_extension`，`supports_materialized_view_introspection` 读取 `pg_matviews`，`supports_sequence_introspection` 读取 `information_schema.sequences`，并在服务器支持时左连接 `pg_sequences`。`supports_extensions` 是核心对第一个探针的拼写，它委托给后者而不是再声明一个 `True`，所以这两个名字是同一个答案：PostgreSQL 安装了什么，而不是方言知道哪些扩展。扩展列表的行通过内省器来自 `pg_extension`。

## 基本用法

### 访问内省器

```python
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
backend = PostgresBackend(
    host="localhost",
    port=5432,
    database="mydb",
    user="postgres",
    password="password"
)
backend.connect()

# Access via introspector attribute
introspector = backend.introspector
```

`backend.introspector` 是 `PostgreSQLIntrospector`；异步后端的则是 `AsyncPostgreSQLIntrospector`。两者都建立在 `PostgreSQLIntrospectorMixin` 之上，所以下文每个方法在两者上都以相同名字存在。

### 获取数据库信息

```python
# Get basic database information
db_info = backend.introspector.get_database_info()
print(f"Database name: {db_info.name}")
print(f"Version: {db_info.version}")
print(f"Encoding: {db_info.encoding}")
print(f"Database size: {db_info.size_bytes} bytes")
```

### 列出表

```python
# List all user tables
tables = backend.introspector.list_tables()
for table in tables:
    print(f"Table: {table.name}, Type: {table.table_type.value}")
    if table.comment:
        print(f"  Comment: {table.comment}")

# Include system tables
all_tables = backend.introspector.list_tables(include_system=True)

# Filter by specific type
base_tables = backend.introspector.list_tables(table_type="BASE TABLE")
views = backend.introspector.list_tables(table_type="VIEW")
materialized_views = backend.introspector.list_tables(table_type="MATERIALIZED VIEW")

# Check if table exists
if backend.introspector.table_exists("users"):
    print("users table exists")

# Get table details
table_info = backend.introspector.get_table_info("users")
if table_info:
    print(f"Table name: {table_info.name}")
    print(f"Estimated rows: {table_info.row_count}")
    print(f"Table size: {table_info.size_bytes} bytes")
```

### 查询列信息

```python
# List all columns of a table
columns = backend.introspector.list_columns("users")
for col in columns:
    nullable = "NOT NULL" if col.nullable.value == "NOT_NULL" else "NULLABLE"
    pk = " [PK]" if col.is_primary_key else ""
    print(f"{col.name}: {col.data_type} {nullable}{pk}")
    if col.comment:
        print(f"  Comment: {col.comment}")

# Get primary key information
pk = backend.introspector.get_primary_key("users")
if pk:
    print(f"Primary key: {[c.name for c in pk.columns]}")

# Get single column information
col_info = backend.introspector.get_column_info("users", "email")
```

### 查询索引

```python
# List all indexes of a table
indexes = backend.introspector.list_indexes("users")
for idx in indexes:
    idx_type = idx.index_type.value if idx.index_type else "BTREE"
    unique = "UNIQUE " if idx.is_unique else ""
    primary = "PRIMARY " if idx.is_primary else ""
    print(f"{primary}{unique}Index: {idx.name} ({idx_type})")
    for col in idx.columns:
        desc = "DESC" if col.is_descending else "ASC"
        nulls = ""
        if col.is_nulls_first is not None:
            nulls = " NULLS FIRST" if col.is_nulls_first else " NULLS LAST"
        print(f"  - {col.name} ({desc}{nulls})")
```

### 查询外键

```python
# List foreign keys of a table
foreign_keys = backend.introspector.list_foreign_keys("posts")
for fk in foreign_keys:
    print(f"FK: {fk.name}")
    print(f"  Columns: {fk.columns} -> {fk.referenced_table}.{fk.referenced_columns}")
    print(f"  ON DELETE: {fk.on_delete.value}")
    print(f"  ON UPDATE: {fk.on_update.value}")
```

### 查询视图

```python
# List all views
views = backend.introspector.list_views()
for view in views:
    print(f"View: {view.name}")

# Get view details
view_info = backend.introspector.get_view_info("user_posts_summary")
if view_info:
    print(f"Definition: {view_info.definition}")
    print(f"Updatable: {view_info.is_updatable}")
```

### 查询触发器

```python
# List all triggers
triggers = backend.introspector.list_triggers()
for trigger in triggers:
    print(f"Trigger: {trigger.name} on {trigger.table_name}")
    print(f"  Event: {trigger.event.value}")
    print(f"  Timing: {trigger.timing.value}")

# List triggers for a specific table
table_triggers = backend.introspector.list_triggers("users")
```

## PostgreSQL 特定行为

### 表类型

PostgreSQL 支持多种表类型：

| 类型 | 说明 |
|------|------|
| `BASE TABLE` | 普通表 |
| `VIEW` | 视图 |
| `MATERIALIZED VIEW` | 物化视图 |
| `FOREIGN TABLE` | 外部表 |
| `PARTITIONED TABLE` | 分区表 |

```python
tables = backend.introspector.list_tables()
for table in tables:
    if table.table_type.value == "MATERIALIZED VIEW":
        print(f"Materialized view: {table.name}")
    elif table.table_type.value == "PARTITIONED TABLE":
        print(f"Partitioned table: {table.name}")
```

### Schema 支持

PostgreSQL 使用 schema 组织数据库对象：

```python
# Default schema is 'public'
tables = backend.introspector.list_tables()  # Queries public schema

# Specify a different schema
tables = backend.introspector.list_tables(schema="analytics")

# Get table details with specific schema
table_info = backend.introspector.get_table_info("users", schema="auth")
```

### 索引类型

PostgreSQL 支持多种高级索引类型：

| 索引类型 | 说明 |
|---------|------|
| `BTREE` | 默认索引类型，适合等值和范围查询 |
| `HASH` | 仅等值查询 |
| `GIN` | 全文搜索、JSONB、数组 |
| `GIST` | 地理空间、全文搜索 |
| `SPGIST` | 空间分区 GiST |
| `BRIN` | 大表块范围索引 |

```python
from rhosocial.activerecord.backend.introspection.types import IndexType

indexes = backend.introspector.list_indexes("articles")
for idx in indexes:
    if idx.index_type == IndexType.GIN:
        print(f"GIN index: {idx.name} (likely for JSONB or full-text search)")
    elif idx.index_type == IndexType.GIST:
        print(f"GiST index: {idx.name} (likely for geospatial data)")
```

`index_type` 起初是目录中的 `pg_am.amname`。核心的 `IndexType` 枚举没有 `SPGIST` 或 `BRIN` 成员，所以解析器把目录中的 `spgist` 映射为 `IndexType.GIST`，把 `brin` 映射为 `IndexType.BTREE`；列表中的值是核心的枚举成员，而不是 `amname` 的拼写。

### 触发器事件

PostgreSQL 触发器支持多种事件：

| 事件 | 说明 |
|------|------|
| `INSERT` | 插入操作 |
| `UPDATE` | 更新操作 |
| `DELETE` | 删除操作 |
| `TRUNCATE` | 清空表 |

```python
triggers = backend.introspector.list_triggers("users")
for trigger in triggers:
    print(f"Trigger {trigger.name}:")
    print(f"  Event: {trigger.event.value}")
    print(f"  Timing: {trigger.timing.value}")  # BEFORE, AFTER, INSTEAD OF
```

### NULLS 排序

PostgreSQL 索引支持 NULLS FIRST/LAST：

```python
indexes = backend.introspector.list_indexes("users")
for idx in indexes:
    for col in idx.columns:
        if col.is_nulls_first is not None:
            nulls_order = "NULLS FIRST" if col.is_nulls_first else "NULLS LAST"
            print(f"{idx.name}.{col.name}: {nulls_order}")
```

## 运维内省

声明层回答服务器*是什么*。这里的六个族中有五个回答它*一直在做什么*，它们读取统计收集器的视图，而不是 `pg_catalog` 本身。第六个是 DDL，它在两方面都是例外：它读取 `pg_catalog`，回答的是一个对象被声明成了什么。这五个族的时态是 PostgreSQL 的：每个计数都是自统计上次被重置以来——一次崩溃或一次 `pg_stat_reset()`——所以零表示窗口内什么都没发生，永远不是“从来没有”。一个没有这句话的计数不是事实，因此这里为这五个族逐一写明时态。

| 方法 | 读取 | 返回 |
|------|------|------|
| `list_unused_indexes(schema=None)` | `pg_stat_user_indexes` 中 `idx_scan = 0` 的行 | schema、表与索引地址 |
| `list_table_usage_stats(schema=None)` | `pg_stat_user_tables` | 顺序扫描与索引扫描、插入/更新/删除计数、活跃行估计 |
| `list_runtime_metrics(schema=None)` | `pg_stat_activity` | 每个后端进程一行 |
| `list_index_usage_stats(schema=None)` | `pg_stat_user_indexes`、`pg_relation_size()` | 扫描次数、读取行数、取回行数、索引字节数 |
| `list_object_dependencies(object_name=None, schema=None)` | `pg_depend`、`pg_class`、`pg_namespace` | 依赖边 |
| `get_ddl_statement(object_name, object_type=None, schema=None)` | `pg_class`、`pg_namespace`；`pg_get_expr` / `pg_get_viewdef` 恢复表达式 | 对象地址与种类；今天没有文本，若组装则标记 `native=False` |

### `list_unused_indexes`

PostgreSQL 没有为未使用索引提供厂商视图，所以该列表由计数计算而来：在所请求的 schema 中取 `pg_stat_user_indexes` 里 `idx_scan = 0` 的行，并连接 `pg_namespace` 取得 schema 名。投影是地址——`schema_name`、`table_name`、`index_name`——因为地址正是该族共同形状所携带的内容，而选出该行的计数按定义就是零。

零计数表示自统计上次重置以来没有发生索引扫描。这是 PostgreSQL 对这个族的时态，也比“未使用”这个词所暗示的断言更弱：一个自崩溃以来、或自 `pg_stat_reset()` 以来未被碰过的索引会报告零。答案只是“值得复查”，永远不是“可以安全删除”。该列表只覆盖统计视图中有行的索引；它没有列出的索引并不因此就被证明使用过。

### `list_table_usage_stats`

该列表读取所请求 schema 的 `pg_stat_user_tables`，收集器见过的每张表一行。它投影 `seq_scan`、`idx_scan`、`n_tup_ins`、`n_tup_upd`、`n_tup_del` 与 `n_live_tup`。视图的其他事实——死元组、最后访问时间——是真实的但没有被投影：它们在 `TableUsageStats` 中没有共同的槽，它们该去哪里是后端读取器的决定，而不是共同形状的决定。没有统计行的表不会出现在列表中，这是诚实的范围，而不是补零的行。`seq_scan` 的零与 `n_tup_ins` 的零支持不同的结论，两者都不能被读成“完全没有活动”。

### `list_runtime_metrics`

该列表读取 `pg_stat_activity`，每个后端进程回答一行。它不接受表名：运行时测量是关于服务器的，表过滤会丢弃那些从来与表无关的行。投影是 `application_name` 作为 `name`、`state` 作为 `value`、`wait_event_type` 作为 `kind`、`wait_event` 作为 `description`、`backend_start` 作为 `source`。`source` 标注的是该行所描述会话的起始时间，而不是计数窗口；计数窗口是上文所述的统计窗口，对每一行都相同。这里刻意不读取 `pg_stat_statements`：它是一个扩展，从一个只是有时安装的目录作答会让该族依赖模块而不是依赖引擎。

### `list_index_usage_stats`

同一个 `pg_stat_user_indexes` 视图，这次读取的是用了多少而不是有没有用过。投影是在一行里的 `idx_scan`（扫描次数）、`idx_tup_read`（读取行数）、`idx_tup_fetch`（取回行数）与 `pg_relation_size(indexrelid)`（索引字节数）。正是这个大小调用让“用了多少”和“占了多少空间”落在同一行，而不是两条需要调用方自己连接的查询。每个计数都是自统计上次重置以来；这里的零是一个测量值，不是缺失。该列表覆盖统计视图中有行的索引。

### `list_object_dependencies`

依赖图从 `pg_depend` 读入，按 `refobjid` 连接 `pg_class`，并按 schema 连接 `pg_namespace`。每一行描述一条某关系作为被引用对象的边；给出 `object_name` 时，列表收窄到该关系的边——也就是“删掉它会坏什么”的问题。投影是 `schema_name`、被引用关系的 `relname` 作为 `referenced`、`deptype` 作为 `kind`、`classid` 作为 `referenced_type`。`action` 保持为空：PostgreSQL 没有逐边的 action 列，依赖对象在删除时的行为是它所属类的属性，所以格式化方法不会从 `deptype` 猜测一个。这里也没有 `deptype` 过滤；kind 被报告出来，让读者自己决定要处理哪些边。

### `get_ddl_statement`

`supports_ddl_extraction` 回答 `True`，也确实为它渲染了语句，但本后端渲染的语句是地址，不是文本。它为指定关系选择 `pg_class.relname`、`pg_namespace.nspname` 与 `pg_class.relkind`，并且不投影 `ddl` 列。解析器会跳过没有文本的行，所以 `get_ddl_statement` 对一张存在的表返回 `None`：这是“无法重现这个”，不是“没有这张表”。`supports_ddl_extraction_native` 回答 `False`，所以需要服务器原生文本的调用方事先就知道这里任何组装都只是重建；`pg_get_expr` 与 `pg_get_viewdef` 能恢复表达式，但一个忠实的 `CREATE TABLE` 还需要存储参数、表空间以及目录只分开保存的逐列子句。CLI 会报告 `Error: no DDL available for '<name>'` 并以 `1` 退出；它不会打印空行，因为一行没有文本的记录会被读成一个答案。

## 列数据剖析

三个数据族中有两个读取 `pg_stats`，也就是 `ANALYZE` 留下的逐列统计，而不是行本身：剖析与值采样。`pg_stats` 每个已分析列一行，所以无论表多大，剖析本身都只花一条目录查询——这是一次目录读取，而不是对行的遍历，核心在此之前还会先读一次廉价的列清单。代价是每个数字都是上一次 `ANALYZE` 的估计，这也是答案要携带来源与收集时间的原因。连接覆盖率是例外：它读取两张表的行，因为 `pg_stats` 只保留每列的一个有界样本，用该样本算出的重叠只会是披着测量的猜测。

| 方法 | 读取 | 返回 |
|------|------|------|
| `list_column_data_profiles(table_name, schema=None)` | 每个已分析列一行 `pg_stats` | 空值比例、不同值计数、极值、行估计、相关性以及保留值列表 |
| `sample_column_values(table_name, column_name, limit=100, schema=None)` | `pg_stats.most_common_vals` 与 `most_common_freqs` | 值与频率对 |
| `estimate_join_coverage(left_table, left_column, right_table, right_column, schema=None)` | 两张表的行 | 两个覆盖率分数 |

### `pg_stats` 不会给出什么

`pg_stats` 把 `histogram_bounds` 与 `most_common_vals` 声明为 `anyarray`——“这个列的类型所对应的数组”的伪类型，并在旁边保留 `most_common_freqs`。PostgreSQL 拒绝对 `anyarray` 列取下标（`cannot subscript type anyarray`），也没有为它定义 `MIN`/`MAX` 聚合，因为没有 btree 操作类的类型没有聚合。`array_to_json` 声明在 `anyarray` 上，所以它是唯一能原样接受这些列的调用；本后端通过它读取全部三个数组。代价是边界以 JSON 标量到达：数字仍是数字，而日期、时间戳或 UUID 会读成它的 ISO 文本。不知道计划时的列类型，就无法从 `pg_stats` 得到带类型的端点，这是目录的限制，而不是库的选择。剖析中的其他内容——`null_frac`、`n_distinct`、`correlation`、`reltuples`——都是原生类型。

### 解析器转换的编码

`n_distinct` 带符号。负值不是计数，而是不同值所占行的比例——当列基数随表增长时 ANALYZE 就这样写——所以解析器用 `reltuples` 乘以它来得到计数。非负值已经是一个计数，会原样携带。没有这个转换，一张百万行表上的唯一列会报告 `-1` 而不是一百万。

`-1` 是“未知”，不是任何东西的负一。`null_frac` 在统计被重置后 ANALYZE 尚未测量的列上是 `-1`，`reltuples` 在规划器从未扫描过的关系上是 `-1`；解析器把两者都变成 `None`。把负比例读成比例、把负行数读成计数的人，会除以一个表示“没人看过”的数字。

`histogram_bounds` 不包含 ANALYZE 作为最常见值保留的那些值。因此它的首尾元素是剩余部分的极值，而不是列的极值：一个最小值同时也是最常见值的状态列，该值在 `most_common_vals` 里，而不在直方图的边缘。解析器在把端点称为 `min_value` 与 `max_value` 之前，会把保留的 `most_common_vals` 合并回去。只有同类才会被比较——两个数字，或两个字符串，后者涵盖文本以及日期和 UUID 的 ISO 拼写——因为 JSON 字符串与 JSON 数字之间没有有意义的顺序。

`pg_stats` 只为 ANALYZE 见过的列保留行。没有统计行的列根本不在剖析列表中，所以答案是每个已分析列一条，而不是每个声明列一条；要把它与 `list_columns` 并排阅读，而不是取代后者。

### 本后端报告的来源

本后端返回的每个剖析，在记录了 `pg_stat_all_tables.last_analyze` 或 `last_autoanalyze` 时携带 `ProfileSource.ESTIMATE`，两者都没有时携带 `ProfileSource.UNKNOWN`。两种情况下数字都是估计；规则是估计必须携带它被收集的时间，而 `UNKNOWN` 是剖析表示其年代未知的方式。

这个时间戳经常缺失，原因值得知道：PostgreSQL 的统计收集器是异步写入 `last_analyze` 的，所以在 `ANALYZE` 之后立刻读取剖析，通常还找不到时间戳。`pg_stat_reset()` 也会在保留统计的同时抹掉时间。两者都不是错误，也都不允许凭空编一个日期。从 `pg_stats` 到 `pg_stat_all_tables` 的连接是 LEFT 连接，正是这个原因：收集器没见过的表，只要它有 `pg_stats` 行，仍会带着未知年代返回。

从未分析过的表没有 `pg_stats` 行，所以它完全不返回行。这是作为缺失的“未测量”，而不是槽位填零的剖析。

```python
from rhosocial.activerecord.backend.introspection.types import ProfileSource

profiles = backend.introspector.list_column_data_profiles("orders")
for profile in profiles:
    print(
        profile.column_name,
        profile.null_fraction,
        profile.distinct_count,
        profile.min_value,
        profile.max_value,
        profile.row_count,
        profile.source,
        profile.collected_at,
    )
    if profile.source is ProfileSource.ESTIMATE:
        print("  measured at", profile.collected_at)
```

### 采样一列的值

`sample_column_values` 投影 ANALYZE 成对保留的两个数组：`most_common_vals` 与 `most_common_freqs`。解析器按位置配对，所以第 i 个频率属于第 i 个值，缺失的频率保持 `None`。两个数组并排保留而不是在 SQL 中展开，因为它们的元素类型不同；分开保留能让值保持列自身的类型。ANALYZE 最多保留 `statistics_target` 个值（默认 100），所以低基数列的列表通常是完整的，而高基数列只是头部的一个样本；返回值少于请求的 `limit` 是正常答案，不是被截断。本后端在读取目录之后按请求的 `limit` 切片，因为在 SQL 中展开两个数组来配对并限制它们会比切片更贵。`frequency` 会被填充；`count` 保持 `None`，因为 `pg_stats` 报告的是比例而不是行数。NULL 是值域的合法成员，会被保留。

### 测量连接覆盖率

PostgreSQL 回答 `supports_join_coverage`，它渲染的语句是这里唯一命名两张用户表而不是一个目录关系的族；两个标识符都由方言加引号。行里携带两个分数，`left_coverage` 与 `right_coverage`，每个都是一列的不同非空值中也出现在另一列不同非空值中的比例。两侧都取不同值，才让这个数字成为值的覆盖而不是行的覆盖：一个重复一千次、只匹配一次的键，否则会被读成覆盖得很好。

NULL 从分子和分母中都被排除。在分子中这个排除是承重的，因为 `SELECT DISTINCT` 会把 NULL 当成一个值；在分母中 `COUNT(DISTINCT ...)` 自己会跳过 NULL，但语句仍显式写出，因为这个比例是“本可以连接的值”的比例。随后 `NULLIF(COUNT(DISTINCT ...), 0)` 让完全没有值的列回答 NULL 而不是 `0.0`：“什么都没匹配”与“没有东西可匹配”保持分开。零重叠是一个测量值，会以 `0.0` 返回。每个分数中的 `* 1.0` 同样是承重的：PostgreSQL 的整数除法会截断，所以 `COUNT(*) / COUNT(...)` 对任何小于一的份额都会回答零。

两个分数都报告，因为关系并不对称。左侧覆盖 `1.0` 而右侧覆盖 `0.2`，表示每个被测值都在另一边存在，而那边的大部分值域没有被涵盖；单一相似度分数会把两者平均，从而两者都不描述。排序规则不会被调和：数据库在某个排序规则下会视为相等的值，在这里可能被计为不同，所以这些分数在排序规则不匹配时是下界，永远不是上界。

代价是一次双重扫描加上两个不同值集合的交集，这也是探针在能力上回答 `True`、而调用从不隐式发生的原因。剖析一张表不会测量任何连接覆盖率，列表表也不会，任何 schema 遍历都不会顺路问它。当行里完全没有分数时，`estimate_join_coverage` 返回 `None`；CLI 会报告这一点并以 `1` 退出，而不是打印一行会被读成“测得零”的 null。

```python
coverage = backend.introspector.estimate_join_coverage(
    "orders", "buyer_id",
    "users", "id",
)
if coverage is None:
    print("no measurement")
else:
    print(coverage.left_coverage, coverage.right_coverage)
```

## 异步 API

异步后端提供相同的内省方法，方法名与同步版本相同：

```python
from rhosocial.activerecord.backend.impl.postgres.backend.async_backend import AsyncPostgresBackend
backend = AsyncPostgresBackend(
    host="localhost",
    port=5432,
    database="mydb",
    user="postgres",
    password="password"
)
await backend.connect()

# Async introspection methods (same names as sync version)
db_info = await backend.introspector.get_database_info()
tables = await backend.introspector.list_tables()
columns = await backend.introspector.list_columns("users")
indexes = await backend.introspector.list_indexes("users")
```

## 缓存管理

内省结果默认会被缓存以提高性能：

```python
# Clear all introspection cache
backend.introspector.clear_cache()

# Invalidate specific scope
from rhosocial.activerecord.backend.introspection.types import IntrospectionScope

# Invalidate all table-related cache
backend.introspector.invalidate_cache(scope=IntrospectionScope.TABLE)

# Invalidate cache for a specific table
backend.introspector.invalidate_cache(
    scope=IntrospectionScope.TABLE,
    name="users"
)

# Invalidate cache for a specific schema
backend.introspector.invalidate_cache(
    scope=IntrospectionScope.TABLE,
    name="users",
    table_name="users"  # Used to qualify schema
)
```

## 版本兼容性

本页上只有一个内省答案取决于服务器版本。`pg_sequences` 在 PostgreSQL 10 才出现，所以 `list_sequences()` 仅在 `server_version` 至少为 `(10, 0, 0)` 时才读取它；在更旧的服务器上，语句会略去该视图，`cache_size` 与 `last_value` 缺席，这是“未测量”而不是零。版本未知时同样采用较窄的形式：内省器不会假定一个它尚未识别的服务器带有更新的目录。

本页其余语句都没有版本门控。`supports_partition_info` 在任何版本上都是 `False`，不只是旧版本：版本差异改变的是分区 DDL，那是另一个协议族，而不是给分区表打标签的内省列表。

## 最佳实践

### 1. 使用缓存

内省操作涉及复杂的 `pg_catalog` 查询，建议利用缓存：

```python
# First query caches the result
tables = backend.introspector.list_tables()

# Subsequent queries return from cache
tables_again = backend.introspector.list_tables()

# Only clear cache when table structure changes
backend.introspector.invalidate_cache(scope=IntrospectionScope.TABLE, name="users")
```

### 2. 指定 Schema

在多 schema 环境中，明确指定 schema：

```python
# Explicitly specify schema to avoid ambiguity
table_info = backend.introspector.get_table_info("users", schema="auth")
```

### 3. 利用索引类型信息

根据索引类型优化查询：

```python
from rhosocial.activerecord.backend.introspection.types import IndexType

indexes = backend.introspector.list_indexes("documents")
for idx in indexes:
    if idx.index_type == IndexType.GIN:
        # GIN indexes are suitable for JSONB or full-text search queries
        print(f"GIN index available: {idx.name}")
```

### 4. 异步环境中的并发查询

```python
async def get_schema_info(backend):
    # Concurrently fetch columns for multiple tables
    import asyncio
    tables = await backend.introspector.list_tables()
    tasks = [
        backend.introspector.list_columns(table.name)
        for table in tables
    ]
    all_columns = await asyncio.gather(*tasks)
    return dict(zip([t.name for t in tables], all_columns))
```

## API 参考

### 核心方法

| 方法 | 说明 | 参数 |
|------|------|------|
| `get_database_info()` | 获取数据库信息 | 无 |
| `list_tables()` | 列出表 | `include_system`, `table_type`, `schema` |
| `get_table_info(name)` | 获取表详情 | `name`, `schema` |
| `table_exists(name)` | 检查表存在 | `name`, `schema` |
| `list_columns(table_name)` | 列出列 | `table_name`, `schema` |
| `get_column_info(table_name, column_name)` | 获取列详情 | `table_name`, `column_name`, `schema` |
| `get_primary_key(table_name)` | 获取主键 | `table_name`, `schema` |
| `list_indexes(table_name)` | 列出索引 | `table_name`, `schema` |
| `get_index_info(table_name, index_name)` | 获取索引详情 | `table_name`, `index_name`, `schema` |
| `index_exists(table_name, index_name)` | 检查索引存在 | `table_name`, `index_name`, `schema` |
| `list_foreign_keys(table_name)` | 列出外键 | `table_name`, `schema` |
| `list_views()` | 列出视图 | `schema` |
| `get_view_info(name)` | 获取视图详情 | `name`, `schema` |
| `view_exists(name)` | 检查视图存在 | `name`, `schema` |
| `list_triggers(table_name)` | 列出触发器 | `table_name`, `schema` |
| `get_trigger_info(table_name, trigger_name)` | 获取触发器详情 | `table_name`, `trigger_name`, `schema` |
| `clear_cache()` | 清除缓存 | 无 |
| `invalidate_cache(scope, ...)` | 使缓存失效 | `scope`, `name`, `table_name` |

### PostgreSQL 专有方法

| 方法 | 说明 | 参数 |
|------|------|------|
| `list_materialized_views()` | 列出物化视图 | `schema`, `include_system` |
| `get_materialized_view_info(name)` | 获取单个物化视图 | `name`, `schema` |
| `materialized_view_exists(name)` | 检查物化视图是否存在 | `name`, `schema` |
| `list_sequences()` | 列出序列 | `schema` |
| `get_sequence_info(name)` | 获取单个序列 | `name`, `schema` |
| `sequence_exists(name)` | 检查序列是否存在 | `name`, `schema` |
| `list_extensions()` | 列出已安装扩展 | 无 |
| `get_extension_info(name)` | 获取单个已安装扩展 | `name` |
| `extension_exists(name)` | 检查扩展是否已安装 | `name` |
| `list_unused_indexes()` | 自统计重置以来没有扫描的索引 | `schema` |
| `list_table_usage_stats()` | 逐表使用计数 | `schema` |
| `list_runtime_metrics()` | 每个后端进程一行 | `schema` |
| `list_index_usage_stats()` | 逐索引计数与大小 | `schema` |
| `list_object_dependencies()` | 依赖边，或单个对象的边 | `object_name`, `schema` |
| `get_ddl_statement(name)` | DDL 文本，或本后端上的 `None` | `name`, `object_type`, `schema` |
| `list_column_data_profiles(table_name)` | 来自 `pg_stats` 的逐列剖析 | `table_name`, `schema` |
| `sample_column_values(table_name, column_name)` | 保留的值与频率 | `table_name`, `column_name`, `limit`, `schema` |
| `estimate_join_coverage(...)` | 两个覆盖率分数 | `left_table`, `left_column`, `right_table`, `right_column`, `schema` |

三个数据方法也声明在核心的内省器上并使用相同名字；PostgreSQL 读取器提供解析，异步孪生类以相同名字 await。

## 命令行内省命令

PostgreSQL 后端提供命令行内省命令，无需编写代码即可查询数据库元数据。

### 基本用法

```bash
# List all tables
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --host localhost --port 5432 --database mydb --user postgres --password

# List all views
python -m rhosocial.activerecord.backend.impl.postgres introspect views \
  --database mydb --user postgres

# Get database information
python -m rhosocial.activerecord.backend.impl.postgres introspect database \
  --database mydb --user postgres

# Include system tables
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --include-system
```

### 连接参数

支持通过命令行参数或环境变量配置连接：

| 参数 | 环境变量 | 说明 |
|------|----------|------|
| `--host` | `POSTGRES_HOST` | 数据库主机（默认 localhost） |
| `--port` | `POSTGRES_PORT` | 数据库端口（默认 5432） |
| `--database` | `POSTGRES_DATABASE` | 数据库名称（必需） |
| `--user` | `POSTGRES_USER` | 用户名（默认 postgres） |
| `--password` | `POSTGRES_PASSWORD` | 密码 |

### 查询表详情

```bash
# Query only column information
python -m rhosocial.activerecord.backend.impl.postgres introspect columns users \
  --database mydb --user postgres

# Query only index information
python -m rhosocial.activerecord.backend.impl.postgres introspect indexes users \
  --database mydb --user postgres

# Query only foreign key information
python -m rhosocial.activerecord.backend.impl.postgres introspect foreign-keys posts \
  --database mydb --user postgres

# Query triggers
python -m rhosocial.activerecord.backend.impl.postgres introspect triggers \
  --database mydb --user postgres

# Query triggers for a specific table
python -m rhosocial.activerecord.backend.impl.postgres introspect triggers users \
  --database mydb --user postgres
```

### Schema 支持

PostgreSQL 支持 schema 参数：

```bash
# Specify schema to query tables
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --schema auth
```

### 查询运维与数据族

六个运维族和三个数据族都是独立的类型：

```bash
# Indexes no query has used since the statistics were reset
python -m rhosocial.activerecord.backend.impl.postgres introspect unused-indexes \
  --database mydb --user postgres

# Per-table usage counters and per-index counters
python -m rhosocial.activerecord.backend.impl.postgres introspect table-stats \
  --database mydb --user postgres
python -m rhosocial.activerecord.backend.impl.postgres introspect index-stats \
  --database mydb --user postgres

# What the server has been doing
python -m rhosocial.activerecord.backend.impl.postgres introspect runtime \
  --database mydb --user postgres

# The dependency graph, or one object's edges
python -m rhosocial.activerecord.backend.impl.postgres introspect dependencies orders \
  --database mydb --user postgres

# The DDL that recreates a table (this backend answers "no DDL available")
python -m rhosocial.activerecord.backend.impl.postgres introspect ddl orders \
  --database mydb --user postgres

# Profiles and samples from pg_stats
python -m rhosocial.activerecord.backend.impl.postgres introspect column-profiles orders \
  --database mydb --user postgres
python -m rhosocial.activerecord.backend.impl.postgres introspect value-samples orders \
  --column status --database mydb --user postgres

# Overlap between two columns' value domains
python -m rhosocial.activerecord.backend.impl.postgres introspect join-coverage orders \
  --column buyer_id --right-table users --right-column id \
  --database mydb --user postgres

# One table's storage split
python -m rhosocial.activerecord.backend.impl.postgres introspect table-storage orders \
  --database mydb --user postgres
```

### 内省类型

| 类型 | 说明 | NAME |
|------|------|------|
| `tables` | schema 中的每种关系：基础表、视图、物化视图、外部表与分区表 | 不接受 |
| `views` | 列出普通视图，或按名称查询一个视图 | 可选：视图名 |
| `columns` | 一张表的列信息 | 必填：表名 |
| `indexes` | 一张表的索引信息 | 必填：表名 |
| `foreign-keys` | 一张表的外键信息 | 必填：表名 |
| `triggers` | 触发器，可缩小到一张表 | 可选：表名 |
| `database` | 服务器版本、字符集，以及当前角色可见的数据库 | 不接受 |
| `extensions` | 已安装的扩展，或按名称查询一个扩展 | 可选：扩展名 |
| `materialized-views` | 物化视图，或按名称查询一个 | 可选：视图名 |
| `sequences` | 序列，或按名称查询一个 | 可选：序列名 |
| `table-storage` | 一张表的存储拆分与行估计 | 必填：表名 |
| `unused-indexes` | 自统计重置以来 `idx_scan = 0` 的索引 | 不接受 |
| `table-stats` | 逐表使用计数 | 不接受 |
| `runtime` | 来自 `pg_stat_activity` 的每个后端进程一行 | 不接受 |
| `index-stats` | 逐索引计数与大小 | 不接受 |
| `dependencies` | 依赖边，或单个对象的边 | 可选：对象名 |
| `ddl` | 重建一张表的 DDL | 必填：表名 |
| `column-profiles` | 来自 `pg_stats` 的逐列剖析 | 必填：表名 |
| `value-samples` | 一列保留的值与频率 | 必填：表名；`--column` 必填 |
| `join-coverage` | 两列值域的重叠 | 必填：左表；`--column`、`--right-table`、`--right-column` 必填 |

这里的十个类型是本页此前列出的十个之外新增的；其中九个就是上文那些族。`table-storage` 向声明层询问一张表的存储拆分；`unused-indexes`、`table-stats`、`runtime`、`index-stats`、`dependencies` 与 `ddl` 是六个运维族；`column-profiles`、`value-samples` 与 `join-coverage` 是三个数据族。每个类型都在 `_TYPE_CAPABILITIES` 中指明决定连接服务器能否回答它的探针开关；对外列出的类型清单本身是静态的，开关在方言适配到服务器之后才被询问。开关为 `False` 的类型会以 `2` 退出并提示 "not supported by this server"，而不是返回空列表。

`NAME` 指明该类型要回答的对象，每种类型对“是否接受名称”只有一个答案。`columns`、`indexes`、`foreign-keys`、`table-storage`、`ddl`、`column-profiles`、`value-samples` 与 `join-coverage` 的名称是它们所涉及的表，必须给出——对 `join-coverage` 而言是左表。`triggers` 接受表名，`dependencies` 接受对象名，`views`、`materialized-views`、`sequences`、`extensions` 接受对象名；不给名称时各自列出整族。`tables`、`database`、`unused-indexes`、`table-stats`、`runtime` 与 `index-stats` 不接受名称：在那里给名称是错误而不是过滤条件，因为这些计数列表回答的是连接的可见范围，而不是单个对象。

有三个类型通过自己的标志询问一列或第二张表。`value-samples` 接受 `--column`，`join-coverage` 接受 `--column`、`--right-table` 与 `--right-column`；缺少其中一个是未完成的调用，会以 `1` 退出。把标志给一个对它没有问题可问的类型，会以 `2` 退出而不是被忽略，规则与 NAME 相同。

`ddl` 是唯一一个可以在一张存在的表上合理报告失败的类型：本后端的投影不携带 DDL 文本，所以 `get_ddl_statement` 返回 `None`，命令会打印 `Error: no DDL available for '<name>'` 并以 `1` 退出。它不会打印空行，因为一行没有文本的记录会被读成一个答案。这里 `supports_ddl_extraction_native` 为 `False`，所以需要服务器原生文本的调用方事先就知道答案是组装，而不是获取。

运行以 `1` 退出表示没能完成——连接失败、查询失败，或名称所指的对象不存在；
以 `2` 退出表示这个请求在此不成立：未知类型、该类型不接受名称，或本服务器
不支持该族。

### 输出格式

```bash
# Table format (default, requires rich library)
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres

# JSON format
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --output json

# CSV format
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --output csv

# TSV format
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --output tsv
```

### 使用异步后端

```bash
# Use --use-async flag to enable async mode
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --use-async
```

### 环境变量配置

可以在环境中设置连接参数，简化命令行调用：

```bash
# Set environment variables
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DATABASE=mydb
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=secret

# Use command directly
python -m rhosocial.activerecord.backend.impl.postgres introspect tables
```
