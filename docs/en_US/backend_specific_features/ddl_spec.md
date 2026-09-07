# DDL Feature Specs

PostgreSQL implements the core DDL feature-spec claiming protocol
(`dialect.build_spec`). This chapter documents which Specs the PostgreSQL
dialect claims, how it translates them, and the PostgreSQL-specific Specs it
adds.

## How claiming works

At `Model.generate_create_table(dialect)` time the generator hands each
declared Spec to `dialect.build_spec(spec)`:

- **Accepted** → the dialect builds and returns an expression-layer instance
  (`TableConstraint` / `IndexDefinition` / `ColumnConstraint` /
  `PartitionClause`), which lands in the `CreateTableExpression`;
- **Not accepted** → returns `None`, and the Spec is silently ignored.

Acceptance scope is the PostgreSQL dialect's own decision.

## Generic Specs

All generic Specs are claimed and translated by the core default:

| Spec | PostgreSQL translation |
|------|------------------------|
| `CheckSpec` | `TableConstraint(CHECK)`, lazy predicates evaluated at build time |
| `UniqueSpec` | `TableConstraint(UNIQUE)` |
| `NotNullSpec` | `ColumnConstraint(NOT NULL)` |
| `PrimaryKeySpec` | column-level PK (single) / table-level composite PK |
| `DefaultSpec` | `ColumnConstraint(DEFAULT)` with a parameterized `Literal` |
| `ForeignKeySpec` | `ForeignKeyConstraint` with referential actions |
| `IndexSpec` / `PartialIndexSpec` | `IndexDefinition` with the partial condition (PostgreSQL supports partial indexes) |
| `JsonColumnSpec` | column type patch → `JsonType`, rendered as native `JSON` |

## PostgreSQL-specific Specs

Defined in `rhosocial.activerecord.backend.impl.postgres.ddl_spec`; claimed
via `isinstance` and translated by `PostgresDDLSpecMixin`. Only the
PostgreSQL dialect claims these.

### Partition Specs

```python
from rhosocial.activerecord.backend.impl.postgres.ddl_spec import (
    PostgresRangePartition,   # also: PostgresListPartition, PostgresHashPartition
)

class Events(ActiveRecord):
    __table_partition__ = [
        PostgresRangePartition(column="created_at"),
    ]
```

Translated to the core `PartitionClause` and rendered as
`PARTITION BY RANGE (...)` (declarative partitioning, PG 10+; HASH requires
11+). See [Partitioning](partition.md) for ATTACH/DETACH and pg_partman.

### Sequence Default

```python
from rhosocial.activerecord.backend.impl.postgres.ddl_spec import (
    PostgresSequenceDefault,
)

class Users(ActiveRecord):
    __table_constraints__ = [
        PostgresSequenceDefault(column="id", sequence="users_id_seq"),
    ]
```

Translates to `DEFAULT nextval('users_id_seq')` via the dedicated
`PostgresSequenceValueExpression` (inline literal rendering — DDL accepts no
bind parameters). When `sequence` is omitted, `<column>_seq` is derived.

### Native-type Column Specs

```python
from rhosocial.activerecord.backend.impl.postgres.ddl_spec import (
    PostgresHstoreColumnSpec,
    PostgresJsonbColumnSpec,
    PostgresTsVectorColumnSpec,
    PostgresNetworkColumnSpec,   # kind: INET / CIDR / MACADDR / MACADDR8
    PostgresArrayColumnSpec,     # element_type: a DataType instance
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

Rendered as `HSTORE`, `JSONB`, `TSVECTOR`, `INET`, `TEXT[]` respectively.
