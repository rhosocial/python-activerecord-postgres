# 高级索引

以下索引全部用表达式节点构造，不手写 `CREATE INDEX` 字符串。访问方法通过
`PostgresIndexType` 选择——它是 `(str, Enum)`，成员值就是 PostgreSQL 写在 `USING` 后面的字面词。
节点里存的仍是字符串，SQL 和跨语言 artifact 保持不变；未知值会在构造或渲染时被拒绝，
不会流到服务端。

```python
from rhosocial.activerecord.backend.impl.postgres.expression import PostgresIndexType

dialect.get_supported_index_types()            # 原生：BTREE HASH GIST GIN SPGIST BRIN
dialect.get_supported_extension_index_types()  # 扩展：HNSW IVFFLAT BLOOM
```

## B-Tree 索引（默认）

适用于等值、排序与范围查询：

```python
from rhosocial.activerecord.backend.expression import CreateIndexStatement
from rhosocial.activerecord.backend.expression.objects import Index, Table

CreateIndexStatement(
    dialect,
    index=Index(dialect, "idx_orders_customer_created"),
    table=Table(dialect, "orders"),
    columns=["customer_id", "created_at"],
    index_type=PostgresIndexType.BTREE,
)
```

## 操作符类（opclass）

opclass 既可以绑到命名列，也可以绑到**函数索引列**——通过 `expression_opclasses`
（表达式节点不可哈希，所以用 `(节点, opclass)` 列表而不是 dict）：

```python
from rhosocial.activerecord.backend.impl.postgres.expression import PostgresCreateIndexExpression

PostgresCreateIndexExpression(
    dialect,
    index=Index(dialect, "idx_orders_tags"),
    table=Table(dialect, "orders"),
    columns=["tags"],
    index_type=PostgresIndexType.GIN,
    opclasses={"tags": "array_ops"},
)
```

## GIN 索引

适用于数组与整个 JSONB 文档：

```python
CreateIndexStatement(
    dialect,
    index=Index(dialect, "idx_events_payload"),
    table=Table(dialect, "events"),
    columns=["payload"],
    index_type=PostgresIndexType.GIN,
)
```

## JSONB 内部键索引

三种写法要区分清楚，PostgreSQL 三种都接受；后端为最常用的两种提供 helper：

```python
from rhosocial.activerecord.backend.impl.postgres.functions import create_jsonb_index

# 子文档 containment：GIN + jsonb_path_ops
create_jsonb_index(dialect, "events", "$.sku")

# 标量键的精确查找/排序：text B-tree（->>）
create_jsonb_index(dialect, "events", "$.user.id", as_text=True)
```

路径以内联转义字面量发出，因为 PostgreSQL 拒绝索引表达式里的绑定参数。
手工构造表达式时用 `json_extract(..., inline_path=True)`。`ARROW` 模式会渲染成正确的键链
（`data->'profile'->>'name'`）且不带参数，同样可用于 DDL。

## GiST / SP-GiST / BRIN

```python
CreateIndexStatement(..., index_type=PostgresIndexType.GIST)    # 范围类型、全文搜索
CreateIndexStatement(..., index_type=PostgresIndexType.SPGIST)  # 分区数据
CreateIndexStatement(..., index_type=PostgresIndexType.BRIN)    # 超大的有序表
```

## 部分索引与唯一索引

```python
from rhosocial.activerecord.backend.expression import Column, Literal
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate

CreateIndexStatement(
    dialect,
    index=Index(dialect, "idx_orders_unpaid"),
    table=Table(dialect, "orders"),
    columns=["customer_id"],
    where=ComparisonPredicate(
        dialect, "=", Column(dialect, "status"), Literal(dialect, "unpaid", inline=True)
    ),
)
```

部分索引的条件值同样内联，原因与 JSON 路径一致：DDL 不能带绑定参数。

## 扩展提供的访问方法

扩展方法共用同一个枚举，渲染前统一校验：

| 扩展 | 方法 | 操作符类 | 示例 |
|---|---|---|---|
| pgvector | `HNSW`、`IVFFLAT` | `vector_cosine_ops`、`vector_l2_ops`、`vector_ip_ops` | `examples/indexes/pgvector_indexes.py` |
| PostGIS | `GIST`、`SPGIST`、`BRIN` | — | `examples/indexes/postgis_indexes.py` |
| pg_trgm | `GIN`、`GIST` | `gin_trgm_ops` / `gist_trgm_ops` | `examples/indexes/pg_trgm_indexes.py` |

扩展安装与能力探测见[插件文档](../plugins/README.md)。

💡 *AI 提示词：* "JSONB 键什么时候该用键级 GIN，什么时候该用 text B-tree？"
