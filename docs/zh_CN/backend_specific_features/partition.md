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
    PostgresCreatePartitionExpression, PartitionValue,
)

# 创建子分区
partition = PostgresCreatePartitionExpression(
    dialect,
    parent_table="orders",
    partition_name="orders_2024_q1",
    partition_type="RANGE",
    bounds="FROM ('2024-01-01') TO ('2024-04-01')",
)
# sql: 'CREATE TABLE "orders_2024_q1" PARTITION OF "orders" FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')'
```

## 分区管理

### ATTACH / DETACH PARTITION

```python
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.partition import (
    PostgresAttachPartitionExpression, PostgresDetachPartitionExpression,
)

# 附加分区
attach = PostgresAttachPartitionExpression(
    dialect, parent_table="orders", partition_name="orders_new",
    partition_type="RANGE", bounds="FROM ('2024-07-01') TO ('2024-10-01')",
)
# sql: 'ALTER TABLE "orders" ATTACH PARTITION "orders_new" FOR VALUES FROM ('2024-07-01') TO ('2024-10-01')'

# 分离分区（PG 14+ 支持 CONCURRENTLY）
detach = PostgresDetachPartitionExpression(
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
