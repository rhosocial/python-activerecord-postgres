# Advanced Indexing

All indexes below are built with expression objects — no hand-written `CREATE INDEX`
strings. The access method is selected with `PostgresIndexType`, a `(str, Enum)` whose
members are the literal words PostgreSQL writes after `USING`. The node still stores a
string, so the SQL and the cross-language artifact are unchanged; an unknown word is
refused at construction or rendering instead of reaching the server.

```python
from rhosocial.activerecord.backend.impl.postgres.expression import PostgresIndexType

dialect.get_supported_index_types()            # native: BTREE HASH GIST GIN SPGIST BRIN
dialect.get_supported_extension_index_types()  # HNSW IVFFLAT BLOOM
```

## B-Tree Index (default)

Equality, ordering and range scans:

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

## Operator classes

An operator class can be attached to a named column, **or to a functional index
column** via `expression_opclasses` (nodes are unhashable, so this is a list of
`(node, opclass)` pairs rather than a dict):

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

## GIN Index

For arrays and whole JSONB documents:

```python
CreateIndexStatement(
    dialect,
    index=Index(dialect, "idx_events_payload"),
    table=Table(dialect, "events"),
    columns=["payload"],
    index_type=PostgresIndexType.GIN,
)
```

## JSONB key indexes

Three shapes are worth distinguishing. PostgreSQL accepts all three; the backend
provides a helper for the two most common ones:

```python
from rhosocial.activerecord.backend.impl.postgres.functions import create_jsonb_index

# Containment on a sub-document: GIN + jsonb_path_ops
create_jsonb_index(dialect, "events", "$.sku")

# Exact lookup / ordering on a scalar key: text B-tree (->>)
create_jsonb_index(dialect, "events", "$.user.id", as_text=True)
```

The path is inlined as an escaped literal because PostgreSQL rejects a bind parameter
inside an index expression. Use `json_extract(..., inline_path=True)` when building the
expression manually. `ARROW` mode renders a proper key chain
(`data->'profile'->>'name'`) and takes no parameters, so it is also safe for DDL.

## GiST / SP-GiST / BRIN

```python
CreateIndexStatement(..., index_type=PostgresIndexType.GIST)    # ranges, full-text
CreateIndexStatement(..., index_type=PostgresIndexType.SPGIST)  # partitioned data
CreateIndexStatement(..., index_type=PostgresIndexType.BRIN)    # huge ordered tables
```

## Partial and unique indexes

```python
from rhosocial.activerecord.backend.expression import Column, Parameter
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate

CreateIndexStatement(
    dialect,
    index=Index(dialect, "idx_orders_unpaid"),
    table=Table(dialect, "orders"),
    columns=["customer_id"],
    where=ComparisonPredicate(
        dialect, "=", Column(dialect, "status"), Parameter(dialect, "unpaid", inline=True)
    ),
)
```

The partial-index value is inlined for the same reason as JSON paths: DDL cannot bind
parameters.

## Extension access methods

Methods supplied by extensions share the same enum and are validated before rendering:

| Extension | Method | Operator class | Example |
|---|---|---|---|
| pgvector | `HNSW`, `IVFFLAT` | `vector_cosine_ops`, `vector_l2_ops`, `vector_ip_ops` | `examples/indexes/pgvector_indexes.py` |
| PostGIS | `GIST`, `SPGIST`, `BRIN` | — | `examples/indexes/postgis_indexes.py` |
| pg_trgm | `GIN`, `GIST` | `gin_trgm_ops` / `gist_trgm_ops` | `examples/indexes/pg_trgm_indexes.py` |

See the [plugin guides](../plugins/README.md) for extension-specific setup.

💡 *AI Prompt:* "When should I choose a key-level GIN index over a text B-tree index for a JSONB key?"
