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
HASH parent pick its own child strategy. Pass a `PartitionClause` to
`PostgresCreatePartitionStatement`; the rendered order is `FOR VALUES` then
`PARTITION BY`.

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

A parent primary key must contain every level's partition key. With a HASH
parent on `tenant_bucket` and RANGE children on `created_at`, the parent key is
`(id, tenant_bucket, created_at)`; PostgreSQL rejects a child that is itself
partitioned when the inherited key omits `created_at`.

## Partition Maintenance

`PostgresPartitionPlan` describes the tree without naming individual
partitions; `PostgresPartitionMaintenance` (and its async twin) reconciles it
with the catalog. The module is owner-agnostic: table names and retention
decisions stay with the application.

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
report = maintenance.ensure(plan)                  # idempotent gap filling
report.missing                                     # (parent, child) pairs
maintenance.list(plan).orphans                     # catalog-only partitions
maintenance.validate(plan).issues                  # gaps / overlaps / mismatches
maintenance.drop_before(plan, cutoff, dry_run=True)# per-branch retention
```

* `ensure` creates only missing partitions. PostgreSQL DDL is transactional, so
  each unit (one branch with its leaves, or all leaves of a flat plan) commits
  independently.
* `list` reads `pg_inherits`/`pg_class` recursively and keeps the server's
  bound text verbatim.
* `drop_before` only drops true leaves; a branch that is itself partitioned is
  never cascaded. It defaults to `dry_run=True`.
* `drop_branch(plan, plan.branches[0])` is the explicit whole-branch removal.
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
