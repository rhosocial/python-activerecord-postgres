# PostgreSQL DDL Operations

The PostgreSQL backend supports the same type-safe DDL expressions as the core library, with PostgreSQL-specific extensions.

## Supported Operations

| Operation | PostgreSQL Support | Notes |
|----------|-------------------|-------|
| `CreateTableExpression` | ✅ Full | PRIMARY KEY, NOT NULL, UNIQUE, etc. |
| `DropTableExpression` | ✅ Full | IF EXISTS, CASCADE, RESTRICT |
| `AlterTableExpression` | ✅ Full | ADD/DROP COLUMN, ALTER COLUMN |
| `CreateIndexExpression` | ✅ Full | Index types (BTREE, HASH, GIN, GiST, BRIN) |
| `DropIndexExpression` | ✅ Full | |
| `CreateViewExpression` | ✅ Full | Materialized views support |
| `DropViewExpression` | ✅ Full | |
| `CreatePartitionExpression` | ✅ Full | RANGE, LIST, HASH partitioning (PG 10+) |
| `AttachPartitionExpression` | ✅ Full | Includes CONCURRENTLY (PG 14+) |
| `DetachPartitionExpression` | ✅ Full | Includes CONCURRENTLY (PG 14+) |
| `AlterIndexExpression` | ✅ Full | RENAME TO, SET TABLESPACE, SET/RESET storage params |
| `ReindexExpression` | ✅ Full | INDEX/TABLE/SCHEMA/DATABASE level, with CONCURRENTLY |

## PostgreSQL-Specific Features

### Partition Support

PostgreSQL 10+ supports declarative partitioning. See [Partition Documentation](../postgres_specific_features/partition.md).

```python
partition = PostgresCreatePartitionExpression(
    dialect, parent_table="orders", partition_name="orders_2024_q1",
    partition_type="RANGE", bounds="FROM ('2024-01-01') TO ('2024-04-01')",
)
```

### pg_partman Extension

See [Partition Documentation](../postgres_specific_features/partition.md) for pg_partman support.

### Index Operation Enhancements

```python
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.index import (
    PostgresAlterIndexExpression, PostgresAlterIndexActionType,
    PostgresReindexExpression,
)

# ALTER INDEX ... RENAME TO
alter = PostgresAlterIndexExpression(
    dialect, index_name="idx_old",
    action=PostgresAlterIndexActionType.RENAME_TO("idx_new"),
)

# REINDEX CONCURRENTLY
reindex = PostgresReindexExpression(
    dialect, target_type="INDEX", target_name="idx_corrupted",
    concurrently=True,
)
```

### Index Types

PostgreSQL supports various index types:

```python
create_idx = CreateIndexExpression(
    dialect,
    index_name="idx_users_name",
    table_name="users",
    columns=["name"],
    index_type="GIN"  # GIN, GiST, BRIN, BTREE, HASH
)
```

### Partial Indexes

PostgreSQL supports partial indexes with WHERE clause:

```python
from rhosocial.activerecord.backend.expression import Column, Literal

create_idx = CreateIndexExpression(
    dialect,
    index_name="idx_active_users",
    table_name="users",
    columns=["email"],
    where=Column(dialect, "status") == Literal(dialect, "active")
)
```

### Schema Support

PostgreSQL supports schemas:

```python
create_table = CreateTableExpression(
    dialect,
    table_name="schema_name.users",
    columns=columns
)
```

## Running the Example

```bash
cd python-activerecord-postgres
source .venv3.8/bin/activate
PYTHONPATH=src python docs/examples/chapter_04_ddl/ddl.py
```

The example tests:
1. Create table with constraints
2. Create table with IF NOT EXISTS
3. Alter table - add column
4. Alter table - drop column
5. Drop table with IF EXISTS
6. Introspection to verify schema changes

> **Note**: PostgreSQL has more powerful DDL support than SQLite. For full PostgreSQL DDL capabilities, refer to [PostgreSQL 16 Documentation](https://www.postgresql.org/docs/16/sql-createtable.html).