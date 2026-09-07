# PostgreSQL Partitioning

PostgreSQL supports declarative partitioning from version 10 onward, with significant enhancements in version 11+.

## Partitioning Strategies

| Strategy | Description | Minimum Version |
|----------|-------------|-----------------|
| RANGE | Range partitioning | PG 10 |
| LIST | List partitioning | PG 10 |
| HASH | Hash partitioning | PG 11 |

## Creating Partitions

### Declarative Partition Specs (model level)

PostgreSQL declarative partitioning can be declared on the model via
backend-defined Specs; the PostgreSQL dialect claims them at
`generate_create_table(dialect)` time and other backends silently ignore them:

```python
from rhosocial.activerecord.backend.impl.postgres.ddl_spec import (
    PostgresRangePartition,  # also: PostgresListPartition, PostgresHashPartition
)

class Orders(ActiveRecord):
    __table_partition__ = [
        PostgresRangePartition(column="created_at"),
    ]

expr = Orders.generate_create_table(dialect)  # PARTITION BY RANGE attached
```

The expression-level path below remains fully supported as the escape hatch.

```python
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.partition import (
    PostgresCreatePartitionExpression,
)

# RANGE partition
partition = PostgresCreatePartitionExpression(
    dialect, parent_table="orders", partition_name="orders_2024_q1",
    partition_type="RANGE",
    partition_values={"from": "2024-01-01", "to": "2024-04-01"},
)
sql, params = partition.to_sql()
# sql: CREATE TABLE orders_2024_q1 PARTITION OF orders RANGE ('2024-01-01', '2024-04-01')
# params: ()

# LIST partition
partition = PostgresCreatePartitionExpression(
    dialect, parent_table="orders", partition_name="orders_active",
    partition_type="LIST",
    partition_values={"values": ["active", "pending"]},
)
sql, params = partition.to_sql()
# sql: CREATE TABLE orders_active PARTITION OF orders LIST ('active', 'pending')
# params: ()
```

## Partition Management

```python
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.partition import (
    PostgresAttachPartitionExpression, PostgresDetachPartitionExpression,
)

# ATTACH PARTITION
attach = PostgresAttachPartitionExpression(
    dialect, parent_table="orders", partition_name="orders_new",
    partition_type="RANGE",
    partition_values={"from": "2024-07-01", "to": "2024-10-01"},
)
sql, params = attach.to_sql()
# sql: ALTER TABLE orders ATTACH PARTITION orders_new RANGE ('2024-07-01', '2024-10-01')
# params: ()

# DETACH PARTITION CONCURRENTLY (PG 14+)
detach = PostgresDetachPartitionExpression(
    dialect, parent_table="orders", partition_name="orders_old",
    concurrently=True,
)
sql, params = detach.to_sql()
# sql: ALTER TABLE orders DETACH PARTITION orders_old CONCURRENTLY
# params: ()
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
sql, params = expr.to_sql()
# sql: CREATE_PARENT(%s, %s, %s, %s, %s)
# params: ('public.orders', 'created_at', '1 month', 'range', 3)

# Run maintenance
expr = run_maintenance(dialect, parent_table="public.orders")
sql, params = expr.to_sql()
# sql: RUN_MAINTENANCE(%s)
# params: ('public.orders',)
```

## Dialect Feature Detection

```python
if dialect.supports_default_partition():
    # PG 11+: DEFAULT partition
if dialect.supports_concurrent_detach():
    # PG 14+: Non-blocking partition detach
```
