# DDL 特征 Spec

PostgreSQL 实现了核心 DDL 特征认领协议（`dialect.build_spec`）。本章说明
PostgreSQL 方言认领哪些 Spec、如何翻译，以及它新增的 PostgreSQL 特定 Spec。

## 认领机制

`Model.generate_create_table(dialect)` 时，生成器把每个声明的 Spec 交给
`dialect.build_spec(spec)`：

- **接受** → 方言构造并返回表达式层实例（`TableConstraint` / `IndexDefinition` /
  `ColumnConstraint` / `PartitionClause`），进入 `CreateTableExpression`；
- **不接受** → 返回 `None`，该 Spec 被静默忽略。

接受范围由 PostgreSQL 方言自行决定。

## 通用 Spec

全部通用 Spec 由核心默认翻译认领：

| Spec | PostgreSQL 翻译 |
|------|-----------------|
| `CheckSpec` | `TableConstraint(CHECK)`，惰性谓词生成时求值 |
| `UniqueSpec` | `TableConstraint(UNIQUE)` |
| `NotNullSpec` | `ColumnConstraint(NOT NULL)` |
| `PrimaryKeySpec` | 单列→列级 PK / 复合→表级 PK |
| `DefaultSpec` | `ColumnConstraint(DEFAULT)`，参数化 `Literal` |
| `ForeignKeySpec` | `ForeignKeyConstraint`（含参照动作） |
| `IndexSpec` / `PartialIndexSpec` | `IndexDefinition` 含部分索引条件（PostgreSQL 支持部分索引） |
| `JsonColumnSpec` | 列类型补丁 → `JsonType`，渲染为原生 `JSON` |

## PostgreSQL 特定 Spec

定义于 `rhosocial.activerecord.backend.impl.postgres.ddl_spec`；以 `isinstance`
认领、由 `PostgresDDLSpecMixin` 翻译。仅 PostgreSQL 方言认领。

### 分区 Spec

```python
from rhosocial.activerecord.backend.impl.postgres.ddl_spec import (
    PostgresRangePartition,   # 还有：PostgresListPartition、PostgresHashPartition
)

class Events(ActiveRecord):
    __table_partition__ = [
        PostgresRangePartition(column="created_at"),
    ]
```

翻译为核心 `PartitionClause`，渲染 `PARTITION BY RANGE (...)`（声明式分区，
PG 10+；HASH 需 11+）。ATTACH/DETACH 与 pg_partman 见[分区](partition.md)。

### 序列默认

```python
from rhosocial.activerecord.backend.impl.postgres.ddl_spec import (
    PostgresSequenceDefault,
)

class Users(ActiveRecord):
    __table_constraints__ = [
        PostgresSequenceDefault(column="id", sequence="users_id_seq"),
    ]
```

经专用 `PostgresSequenceValueExpression` 翻译为 `DEFAULT nextval('users_id_seq')`
（内联字面量渲染——DDL 不接受绑定参数）。`sequence` 省略时派生 `<column>_seq`。

### 原生类型列 Spec

```python
from rhosocial.activerecord.backend.impl.postgres.ddl_spec import (
    PostgresHstoreColumnSpec,
    PostgresJsonbColumnSpec,
    PostgresTsVectorColumnSpec,
    PostgresNetworkColumnSpec,   # kind: INET / CIDR / MACADDR / MACADDR8
    PostgresArrayColumnSpec,     # element_type: DataType 实例
)
from rhosocial.activerecord.backend.expression.types import TextType

class T(ActiveRecord):
    __table_constraints__ = [
        PostgresHstoreColumnSpec("attrs"),
        PostgresJsonbColumnSpec("data"),
        PostgresTsVectorColumnSpec("doc"),
        PostgresNetworkColumnSpec("ip", kind="INET"),
        PostgresArrayColumnSpec("tags", TextType()),
    ]
```

分别渲染为 `HSTORE`、`JSONB`、`TSVECTOR`、`INET`、`TEXT[]`。
