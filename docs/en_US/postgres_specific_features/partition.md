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
    PostgresCreatePartitionStatement,
)

partition = PostgresCreatePartitionStatement(
    dialect, parent_table="orders", partition_name="orders_2024_q1",
    partition_type="RANGE",
    partition_values={"from": "2024-01-01", "to": "2024-04-01"},
)
```

## Nested Partitions

PostgreSQL lets a partition be partitioned again, and it lets each branch of a
HASH parent pick its own child strategy. Pass a strategy clause such as
`PartitionByRangeClause` to `PostgresCreatePartitionStatement`; the rendered
order is `FOR VALUES` then `PARTITION BY`.

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

A parent primary key must contain every level's partition key. With a HASH
parent on `tenant_bucket` and RANGE children on `created_at`, the parent key is
`(id, tenant_bucket, created_at)`; PostgreSQL rejects a child that is itself
partitioned when the inherited key omits `created_at`.

## Partition Plans, Suggestions and Introspection

`PostgresPartitionPlan` describes a tree as data -- parent, partition keys,
policies and naming -- without hard-coding individual partition names. The
plan grammar (`partition/plan.py` and `partition/bounds.py`) derives bounds and
labels. There is no backend maintenance executor: the "how" of every operation
comes from the backend suggestion protocols on `PostgresDialect`, and the SQL
spelling comes from the DDL expression classes. Live tree state comes from the
introspection layer. The plan is owner-agnostic: table names and retention
decisions stay with the application.

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

specs = tuple(plan.iter_specs())                   # derived branch/leaf specs

# A strategy is a clause expression: the class names the word, the keys name
# the partition columns.
def _strategy(clause_type, column):
    return clause_type(dialect, [Column(dialect, column)])

build = dialect.suggest_partition_build(_strategy(PartitionByRangeClause, "created_at"), plan)
adds = dialect.suggest_partition_add(_strategy(PartitionByHashClause, "tenant_bucket"), plan)
retention = dialect.suggest_partition_retention(
    _strategy(PartitionByRangeClause, "created_at"), plan
)
```

* `iter_specs()` materializes the tree into concrete specs; each spec carries
  the parent, method, bounds and level needed to render DDL.
* `suggest_partition_build` / `suggest_partition_add` / `suggest_partition_drop`
  and the other suggestion protocols return DDL expression nodes; execute the
  nodes the same way as any other backend expression.
* `backend.introspector.get_partition_state("logs")` (introspection) reads the
  parent and its children from `pg_inherits` / `pg_class` and returns one
  `PartitionState`: the parent's `scheme` and key columns, plus a
  `PartitionBound` per child whose `lower`/`upper` (RANGE), `values` (LIST),
  `modulus`/`remainder` (HASH) and `is_default` (DEFAULT) carry the structured
  facts. The server's own bound spelling stays verbatim in
  `state.raw["partitions"]` as `{name, text}` rows.
* A flat plan (`policy=RangePolicy(...)` with no `branches`) produces the
  single-level shape used by monthly log tables.


## Partition Management

PostgreSQL does not support `ATTACH PARTITION CONCURRENTLY`. PostgreSQL 14+ supports
`DETACH PARTITION CONCURRENTLY`, with `FINALIZE` for completing a pending detach.

```python
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.partition import (
    PostgresAttachPartitionStatement, PostgresDetachPartitionStatement,
)

# ATTACH PARTITION
attach = PostgresAttachPartitionStatement(
    dialect, parent_table="orders", partition_name="orders_new",
    partition_type="RANGE",
    partition_values={"from": "2024-07-01", "to": "2024-10-01"},
)

# DETACH PARTITION CONCURRENTLY (PG 14+)
detach = PostgresDetachPartitionStatement(
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
