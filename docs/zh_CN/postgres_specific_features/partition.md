# PostgreSQL 分区

PostgreSQL 自 10 版本起支持声明式分区，11+ 版本功能显著增强。

## 分区策略

| 策略 | 说明 | 最低版本 |
|------|------|---------|
| RANGE | 范围分区 | PG 10 |
| LIST | 列表分区 | PG 10 |
| HASH | 哈希分区 | PG 11 |

## 创建分区表

```python
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.partition import (
    PostgresCreatePartitionStatement, PostgresPartitionValue,
)

# 创建子分区
partition = PostgresCreatePartitionStatement(
    dialect,
    parent_table="orders",
    partition_name="orders_2024_q1",
    partition_type="RANGE",
    partition_values={"from": "2024-01-01", "to": "2024-04-01"},
)
# sql: 'CREATE TABLE "orders_2024_q1" PARTITION OF "orders" FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')'
```

## 多级（子）分区

PostgreSQL 允许一个分区继续分区，且 HASH 父表的每个一级分支可以各自选择
二级策略。把策略子句（如 `PartitionByRangeClause`）传给
`PostgresCreatePartitionStatement`，渲染顺序为 `FOR VALUES` → `PARTITION BY`。

```python
from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.expression.statements import PartitionByRangeClause

# CREATE TABLE logs_p0 PARTITION OF logs
#   FOR VALUES WITH (MODULUS 4, REMAINDER 0)
#   PARTITION BY RANGE (created_at)
branch = PostgresCreatePartitionStatement(
    dialect, parent_table="logs", partition_name="logs_p0",
    partition_type="HASH", partition_values={"modulus": 4, "remainder": 0},
    partition_clause=PartitionByRangeClause(
        dialect, [Column(dialect, "created_at")],
    ),
)

# CREATE TABLE logs_p0_2026_01 PARTITION OF logs_p0
#   FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2026-02-01 00:00:00+00')
leaf = PostgresCreatePartitionStatement(
    dialect, parent_table="logs_p0", partition_name="logs_p0_2026_01",
    partition_type="RANGE",
    partition_values={"from": "2026-01-01 00:00:00+00",
                      "to": "2026-02-01 00:00:00+00"},
)
```

父表主键必须包含**每一级**的分区键。若一级按 `tenant_bucket` HASH、二级按
`created_at` RANGE，父表主键应为 `(id, tenant_bucket, created_at)`；当继承的
唯一键缺少 `created_at` 时，PostgreSQL 会拒绝“本身又分区”的一级子表。

## 分区计划、建议与内省

`PostgresPartitionPlan` 只把树描述为数据（父表、分区键、策略、命名），不写死具体
分区名。边界与标签由计划语法（`partition/plan.py` 与 `partition/bounds.py`）推导。
后端不再有分区维护执行器：所有操作的“怎么做”由 `PostgresDialect` 上的后端建议
协议提供，SQL 拼写由 DDL 表达式类负责；线上树状态由内省层提供。计划不绑定任何
业务表名或保留策略，这些仍由应用决定。

```python
from datetime import datetime, timezone
from rhosocial.activerecord.backend.expression.core import Column
from rhosocial.activerecord.backend.expression.statements import (
    PartitionByHashClause, PartitionByRangeClause,
)
from rhosocial.activerecord.backend.impl.postgres.partition import (
    HashBranch, PostgresPartitionPlan, RangePolicy,
)

start = datetime(2026, 1, 1, tzinfo=timezone.utc)
plan = PostgresPartitionPlan(
    parent="logs", method="HASH", columns=("tenant_bucket",), modulus=4,
    branches=(
        HashBranch(0, RangePolicy(columns=("created_at",), start=start,
                                  interval="month", count=12)),
        HashBranch(1, RangePolicy(columns=("created_at",), start=start,
                                  interval="quarter", count=4)),
        HashBranch(2, RangePolicy(columns=("created_at",), start=start,
                                  interval="year", count=2)),
        HashBranch(3, RangePolicy(columns=("created_at",),
                                  bounds=((start, start.replace(day=16)),))),
    ),
)

specs = tuple(plan.iter_specs())                   # 推导出的分支/叶子 spec

# 策略现在由子句表达：类名给出策略词，keys 给出分区键。
def _strategy(clause_type, column):
    return clause_type(dialect, [Column(dialect, column)])

build = dialect.suggest_partition_build(_strategy(PartitionByRangeClause, "created_at"), plan)
adds = dialect.suggest_partition_add(_strategy(PartitionByHashClause, "tenant_bucket"), plan)
retention = dialect.suggest_partition_retention(
    _strategy(PartitionByRangeClause, "created_at"), plan
)
```

* `iter_specs()` 把树展开为具体 spec；每个 spec 携带渲染 DDL 所需的父表、
  方法、边界与层级。
* `suggest_partition_build` / `suggest_partition_add` / `suggest_partition_drop`
  及其它建议协议返回 DDL 表达式节点，按普通后端表达式的方式执行即可。
* `backend.introspector.get_partition_state("logs")`（内省）从 `pg_inherits` /
  `pg_class` 读取父表及其子分区，返回一个 `PartitionState`：父表的 `scheme`
  与分区键列，以及每个子分区一个 `PartitionBound`，其 `lower`/`upper`（RANGE）、
  `values`（LIST）、`modulus`/`remainder`（HASH）与 `is_default`（DEFAULT）
  承载结构化事实；服务器原始的 bound 拼写保留在 `state.raw["partitions"]` 的
  `{name, text}` 行中。
* 单层计划（只给 `policy=RangePolicy(...)`、不给 `branches`）生成按月日志表
  所需的扁平形态。


## 分区管理

PostgreSQL 不支持 `ATTACH PARTITION CONCURRENTLY`。PostgreSQL 14+ 支持
`DETACH PARTITION CONCURRENTLY`，并可使用 `FINALIZE` 完成待处理的分离。

### ATTACH / DETACH PARTITION

```python
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.partition import (
    PostgresAttachPartitionStatement, PostgresDetachPartitionStatement,
)

# 附加分区
attach = PostgresAttachPartitionStatement(
    dialect, parent_table="orders", partition_name="orders_new",
    partition_type="RANGE",
    partition_values={"from": "2024-07-01", "to": "2024-10-01"},
)
# sql: 'ALTER TABLE "orders" ATTACH PARTITION "orders_new" FOR VALUES FROM ('2024-07-01') TO ('2024-10-01')'

# 分离分区（PG 14+ 支持 CONCURRENTLY）
detach = PostgresDetachPartitionStatement(
    dialect, parent_table="orders", partition_name="orders_old",
    concurrently=True,
)
# sql: 'ALTER TABLE "orders" DETACH PARTITION "orders_old" CONCURRENTLY'
```

### 方言检查

```python
if dialect.supports_default_partition():
    # PG 11+: DEFAULT 分区

if dialect.supports_concurrent_detach():
    # PG 14+: 非阻塞分区分离

if dialect.supports_partition_key_update():
    # PG 11+: 键更新时自动行移动
```

## pg_partman 扩展

pg_partman 是 PostgreSQL 分区管理扩展，支持自动创建和维护分区。

### 安装

```sql
CREATE EXTENSION pg_partman;
```

### 创建分区父表

```python
from rhosocial.activerecord.backend.impl.postgres.functions.pg_partman import create_parent

# 创建按月分区的父表
expr = create_parent(
    dialect,
    parent_table="public.orders",
    control="created_at",
    interval="1 month",
    partition_type="range",
    premake=3,  # 提前创建 3 个分区
)
# 生成: SELECT partman.create_parent('public.orders', 'created_at', '1 month', 'range', p_premake := 3)
```

### 运行维护

```python
from rhosocial.activerecord.backend.impl.postgres.functions.pg_partman import run_maintenance

expr = run_maintenance(dialect, parent_table="public.orders")
# 生成: SELECT partman.run_maintenance('public.orders')
```

### 分区快照创建辅助

```python
from rhosocial.activerecord.backend.impl.postgres.named_expressions.partition import (
    create_next_monthly_partition, create_range_partition_for_month,
    create_range_partitions_for_interval,
)

# 创建下个月分区
partition_expr = create_next_monthly_partition(dialect, "orders")

# 创建指定月份分区
partition_expr = create_range_partition_for_month(dialect, "orders", 2024, 10)

# 批量创建时间范围内的分区
for expr in create_range_partitions_for_interval(dialect, "orders", "2024-01-01", "2025-01-01", "1 month"):
    # 执行每个 CREATE TABLE partition OF ...
    pass
```
