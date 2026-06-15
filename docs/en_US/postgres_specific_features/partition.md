# PostgreSQL Partitioning

PostgreSQL supports declarative partitioning from version 10 onward, with significant enhancements in version 11+.

## Partitioning Strategies

| Strategy | Description | Minimum Version |
|----------|-------------|-----------------|
| RANGE | Range partitioning | PG 10 |
| LIST | List partitioning | PG 10 |
| HASH | Hash partitioning | PG 11 |

## Creating Partitions

```python
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.partition import (
    PostgresCreatePartitionExpression,
)

partition = PostgresCreatePartitionExpression(
    dialect, parent_table="orders", partition_name="orders_2024_q1",
    partition_type="RANGE", bounds="FROM ('2024-01-01') TO ('2024-04-01')",
)
```

## Partition Management

```python
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.partition import (
    PostgresAttachPartitionExpression, PostgresDetachPartitionExpression,
)

# ATTACH PARTITION
attach = PostgresAttachPartitionExpression(
    dialect, parent_table="orders", partition_name="orders_new",
    partition_type="RANGE", bounds="FROM ('2024-07-01') TO ('2024-10-01')",
)

# DETACH PARTITION CONCURRENTLY (PG 14+)
detach = PostgresDetachPartitionExpression(
    dialect, parent_table="orders", partition_name="orders_old",
    concurrently=True,
)
```

## pg_partman Extension

```python
from rhosocial.activerecord.backend.impl.postgres.functions.pg_partman import create_parent, run_maintenance

# Create parent table with monthly partitioning
expr = create_parent(
    dialect, parent_table="public.orders",
    control="created_at", interval="1 month",
    partition_type="range", premake=3,
)

# Run maintenance
expr = run_maintenance(dialect, parent_table="public.orders")
```

## Dialect Feature Detection

```python
if dialect.supports_default_partition():
    # PG 11+: DEFAULT partition
if dialect.supports_concurrent_detach():
    # PG 14+: Non-blocking partition detach
```
