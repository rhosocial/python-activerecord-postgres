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
二级策略。把 `PartitionClause` 传给 `PostgresCreatePartitionStatement`，
渲染顺序为 `FOR VALUES` → `PARTITION BY`。

```python
from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.expression.statements import (
    PartitionClause, PartitionStrategy,
)

# CREATE TABLE logs_p0 PARTITION OF logs
#   FOR VALUES WITH (MODULUS 4, REMAINDER 0)
#   PARTITION BY RANGE (created_at)
branch = PostgresCreatePartitionStatement(
    dialect, parent_table="logs", partition_name="logs_p0",
    partition_type="HASH", partition_values={"modulus": 4, "remainder": 0},
    partition_clause=PartitionClause(
        dialect, PartitionStrategy.RANGE, [Column(dialect, "created_at")],
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

## 分区维护

`PostgresPartitionPlan` 只描述树（父表、分区键、分支策略、命名），不写死具体
分区名；`PostgresPartitionMaintenance`（及其异步版本）把它与目录状态对齐。
模块不绑定任何业务表名或保留策略，这些仍由应用决定。

```python
from datetime import datetime, timezone
from rhosocial.activerecord.backend.impl.postgres.partition import (
    HashBranch, PostgresPartitionMaintenance, PostgresPartitionPlan, RangePolicy,
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

maintenance = PostgresPartitionMaintenance(backend)
report = maintenance.ensure(plan)                   # 幂等补建缺失分区
report.missing                                      # (父表, 缺失分区) 列表
maintenance.list(plan).orphans                      # 计划外的分区
maintenance.validate(plan).issues                   # 缺口 / 重叠 / 策略不符
maintenance.drop_before(plan, cutoff, dry_run=True) # 按分支策略回收
```

* `ensure` 只补建缺失项；PostgreSQL 的 DDL 是事务性的，每个单元（一个一级
  分支及其叶子，或单层计划的全部叶子）独立提交。
* `list` 递归读取 `pg_inherits` / `pg_class`，并原样保留服务器给出的 bound 文本。
* `drop_before` 只删除真正的叶子；自身已分区的一级分支不会被级联删除，默认
  `dry_run=True`。
* `drop_branch(plan, plan.branches[0])` 是删除整个一级分支的显式操作。
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
