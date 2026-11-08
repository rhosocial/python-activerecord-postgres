# Database Introspection

The PostgreSQL backend provides complete database introspection capabilities using `pg_catalog` system tables for querying database structure metadata.

## Overview

The PostgreSQL introspection system is accessible via `backend.introspector` and provides:

- **Database Information**: Name, version, encoding, database size
- **Table Information**: Table list, table details (including materialized views, partitioned tables, foreign tables)
- **Column Information**: Column name, data type, nullability, default value
- **Index Information**: Index name, columns, uniqueness, index type (BTREE, GIN, GiST, etc.)
- **Foreign Key Information**: Referenced table, column mapping, update/delete actions
- **View Information**: View definition SQL
- **Trigger Information**: Trigger event, execution timing
- **Operational Measurements**: Unused indexes, table and index usage counters, live runtime metrics, dependencies, and DDL
- **Column Data Profiles**: Per-column statistics from `pg_stats`, kept value samples, and join coverage

## What PostgreSQL Answers

Core declares twenty-one `supports_*` probes, and each asks a question about the
engine rather than about the library. PostgreSQL answers nineteen of them `True`.
The table below is this backend's answer to every one, in the order core declares
them, with the catalogue view or statement each answer is read from.

| Probe | Answer | PostgreSQL's answer comes from |
|-------|--------|-------------------------------|
| `supports_introspection` | `True` | This dialect's own declaration; all seven introspection scopes are declared |
| `supports_database_info` | `True` | `pg_database`, `version()`, `pg_database_size()` |
| `supports_table_introspection` | `True` | `pg_class` / `pg_namespace`, with `pg_stat_user_tables` left-joined for live rows |
| `supports_column_introspection` | `True` | `pg_attribute`, `pg_attrdef`, `pg_index`, `information_schema.columns` |
| `supports_index_introspection` | `True` | `pg_index`, `pg_class`, `pg_am` |
| `supports_foreign_key_introspection` | `True` | `pg_constraint` (`contype = 'f'`) |
| `supports_view_introspection` | `True` | `pg_class` (`relkind = 'v'`), `pg_get_viewdef()` |
| `supports_trigger_introspection` | `True` | `pg_trigger`, `pg_class`, `pg_proc` |
| `supports_runtime_stats` | `True` | `pg_stat_activity` |
| `supports_index_stats` | `True` | `pg_stat_user_indexes`, `pg_relation_size()` |
| `supports_table_stats` | `True` | `pg_stat_user_tables` |
| `supports_unused_indexes_detection` | `True` | `pg_stat_user_indexes.idx_scan` |
| `supports_partition_info` | `False` | Inherited core default; no partition-information formatter (see below) |
| `supports_object_dependencies` | `True` | `pg_depend`, `pg_class`, `pg_namespace` |
| `supports_column_data_profile` | `True` | `pg_stats` |
| `supports_column_value_sample` | `True` | `pg_stats.most_common_vals` |
| `supports_join_coverage` | `True` | Counts distinct values on both sides; no catalogue source |
| `supports_ddl_extraction` | `True` | `pg_class` / `pg_namespace`; the projection carries no DDL text |
| `supports_extensions` | `True` | `pg_extension`, through `supports_extension_introspection` |
| `supports_table_storage_introspection` | `True` | `pg_total_relation_size()`, `pg_relation_size()`, `pg_indexes_size()` |
| `supports_ddl_extraction_native` | `False` | No single native DDL retrieval call (see below) |

Two answers are `False`. One reason is written in the probe itself; the other
is inherited from core's default, and what the default leaves out is stated below.

### The Two False Answers

`supports_partition_info` answers `False` through core's default: this dialect
does not override the probe and declares no partition-information query
expression or formatter. A partitioned table is still answered — the table
listing reports it with the `table_type` label `PARTITIONED TABLE`, from the
`p` relkind code — but that is the relation's kind, not the partition tree:
nothing here reports the parent, the children or the bounds. Partition DDL is a
separate protocol family (`supports_hash_partitioning`, `supports_attach_partition`
and their neighbours), not this introspection probe.

`supports_ddl_extraction_native` answers `False`. The probe's docstring is the
reason: PostgreSQL has no single native DDL retrieval call. `pg_get_expr` and
`pg_get_viewdef` recover expressions, but a faithful `CREATE TABLE` also needs
storage parameters, the tablespace and per-column clauses the catalogue keeps
only in parts, so what this backend can offer is an assembly, and `native=False`
is how an assembly is marked. `supports_ddl_extraction` stays `True` because the
family is declared and a statement renders for it; the statement itself is
described under `get_ddl_statement` below.

### The Probes PostgreSQL Adds

Three more probes belong to this backend rather than to core's protocol, and
each answers `True`: `supports_extension_introspection` reads `pg_extension`,
`supports_materialized_view_introspection` reads `pg_matviews`, and
`supports_sequence_introspection` reads `information_schema.sequences` with
`pg_sequences` left-joined where the server has it. `supports_extensions` is
core's spelling of the first one and delegates to it rather than declaring a
second `True`, so the two names are one answer: what PostgreSQL has installed,
not which extensions the dialect knows about. The extension listing's rows come
from `pg_extension` through the introspector.

## Basic Usage

### Accessing the Introspector

```python
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
backend = PostgresBackend(
    host="localhost",
    port=5432,
    database="mydb",
    user="postgres",
    password="password"
)
backend.connect()

# Access via introspector attribute
introspector = backend.introspector
```

`backend.introspector` is a `PostgreSQLIntrospector`; the async backend's is an
`AsyncPostgreSQLIntrospector`. Both are built on `PostgreSQLIntrospectorMixin`,
so every method below exists on both under the same name.

### Getting Database Information

```python
# Get basic database information
db_info = backend.introspector.get_database_info()
print(f"Database name: {db_info.name}")
print(f"Version: {db_info.version}")
print(f"Encoding: {db_info.encoding}")
print(f"Database size: {db_info.size_bytes} bytes")
```

### Listing Tables

```python
# List all user tables
tables = backend.introspector.list_tables()
for table in tables:
    print(f"Table: {table.name}, Type: {table.table_type.value}")
    if table.comment:
        print(f"  Comment: {table.comment}")

# Include system tables
all_tables = backend.introspector.list_tables(include_system=True)

# Filter by specific type
base_tables = backend.introspector.list_tables(table_type="BASE TABLE")
views = backend.introspector.list_tables(table_type="VIEW")
materialized_views = backend.introspector.list_tables(table_type="MATERIALIZED VIEW")

# Check if table exists
if backend.introspector.table_exists("users"):
    print("users table exists")

# Get table details
table_info = backend.introspector.get_table_info("users")
if table_info:
    print(f"Table name: {table_info.name}")
    print(f"Estimated rows: {table_info.row_count}")
    print(f"Table size: {table_info.size_bytes} bytes")
```

### Querying Column Information

```python
# List all columns of a table
columns = backend.introspector.list_columns("users")
for col in columns:
    nullable = "NOT NULL" if col.nullable.value == "NOT_NULL" else "NULLABLE"
    pk = " [PK]" if col.is_primary_key else ""
    print(f"{col.name}: {col.data_type} {nullable}{pk}")
    if col.comment:
        print(f"  Comment: {col.comment}")

# Get primary key information
pk = backend.introspector.get_primary_key("users")
if pk:
    print(f"Primary key: {[c.name for c in pk.columns]}")

# Get single column information
col_info = backend.introspector.get_column_info("users", "email")
```

### Querying Indexes

```python
# List all indexes of a table
indexes = backend.introspector.list_indexes("users")
for idx in indexes:
    idx_type = idx.index_type.value if idx.index_type else "BTREE"
    unique = "UNIQUE " if idx.is_unique else ""
    primary = "PRIMARY " if idx.is_primary else ""
    print(f"{primary}{unique}Index: {idx.name} ({idx_type})")
    for col in idx.columns:
        desc = "DESC" if col.is_descending else "ASC"
        nulls = ""
        if col.is_nulls_first is not None:
            nulls = " NULLS FIRST" if col.is_nulls_first else " NULLS LAST"
        print(f"  - {col.name} ({desc}{nulls})")
```

### Querying Foreign Keys

```python
# List foreign keys of a table
foreign_keys = backend.introspector.list_foreign_keys("posts")
for fk in foreign_keys:
    print(f"FK: {fk.name}")
    print(f"  Columns: {fk.columns} -> {fk.referenced_table}.{fk.referenced_columns}")
    print(f"  ON DELETE: {fk.on_delete.value}")
    print(f"  ON UPDATE: {fk.on_update.value}")
```

### Querying Views

```python
# List all views
views = backend.introspector.list_views()
for view in views:
    print(f"View: {view.name}")

# Get view details
view_info = backend.introspector.get_view_info("user_posts_summary")
if view_info:
    print(f"Definition: {view_info.definition}")
    print(f"Updatable: {view_info.is_updatable}")
```

### Querying Triggers

```python
# List all triggers
triggers = backend.introspector.list_triggers()
for trigger in triggers:
    print(f"Trigger: {trigger.name} on {trigger.table_name}")
    print(f"  Event: {trigger.event.value}")
    print(f"  Timing: {trigger.timing.value}")

# List triggers for a specific table
table_triggers = backend.introspector.list_triggers("users")
```

## PostgreSQL-Specific Behaviors

### Table Types

PostgreSQL supports multiple table types:

| Type | Description |
|------|-------------|
| `BASE TABLE` | Regular table |
| `VIEW` | View |
| `MATERIALIZED VIEW` | Materialized view |
| `FOREIGN TABLE` | Foreign table |
| `PARTITIONED TABLE` | Partitioned table |

```python
tables = backend.introspector.list_tables()
for table in tables:
    if table.table_type.value == "MATERIALIZED VIEW":
        print(f"Materialized view: {table.name}")
    elif table.table_type.value == "PARTITIONED TABLE":
        print(f"Partitioned table: {table.name}")
```

### Schema Support

PostgreSQL uses schemas to organize database objects:

```python
# Default schema is 'public'
tables = backend.introspector.list_tables()  # Queries public schema

# Specify a different schema
tables = backend.introspector.list_tables(schema="analytics")

# Get table details with specific schema
table_info = backend.introspector.get_table_info("users", schema="auth")
```

### Index Types

PostgreSQL supports multiple advanced index types:

| Index Type | Description |
|------------|-------------|
| `BTREE` | Default index type, suitable for equality and range queries |
| `HASH` | Equality queries only |
| `GIN` | Full-text search, JSONB, arrays |
| `GIST` | Geospatial, full-text search |
| `SPGIST` | Space-partitioned GiST |
| `BRIN` | Block range index for large tables |

```python
from rhosocial.activerecord.backend.introspection.types import IndexType

indexes = backend.introspector.list_indexes("articles")
for idx in indexes:
    if idx.index_type == IndexType.GIN:
        print(f"GIN index: {idx.name} (likely for JSONB or full-text search)")
    elif idx.index_type == IndexType.GIST:
        print(f"GiST index: {idx.name} (likely for geospatial data)")
```

`index_type` starts as the catalogue's `pg_am.amname`. Core's `IndexType` enum
has no `SPGIST` or `BRIN` member, so the parser maps the catalogue's `spgist`
to `IndexType.GIST` and `brin` to `IndexType.BTREE`; the value in the listing is
a core enum member, not the `amname` spelling.

### Trigger Events

PostgreSQL triggers support multiple events:

| Event | Description |
|-------|-------------|
| `INSERT` | Insert operation |
| `UPDATE` | Update operation |
| `DELETE` | Delete operation |
| `TRUNCATE` | Truncate table |

```python
triggers = backend.introspector.list_triggers("users")
for trigger in triggers:
    print(f"Trigger {trigger.name}:")
    print(f"  Event: {trigger.event.value}")
    print(f"  Timing: {trigger.timing.value}")  # BEFORE, AFTER, INSTEAD OF
```

### NULLS Ordering

PostgreSQL indexes support NULLS FIRST/LAST:

```python
indexes = backend.introspector.list_indexes("users")
for idx in indexes:
    for col in idx.columns:
        if col.is_nulls_first is not None:
            nulls_order = "NULLS FIRST" if col.is_nulls_first else "NULLS LAST"
            print(f"{idx.name}.{col.name}: {nulls_order}")
```

## Operational Introspection

The declaration layer answers what the server *is*. The six families here answer
what it has *been doing*, and they read the statistics collector's views rather
than `pg_catalog` proper. The tense is PostgreSQL's: every counter is since the
statistics were last reset — a crash or a `pg_stat_reset()` — so a zero means
nothing happened in the window, never nothing ever. A number without that
sentence is not a fact, which is why the tense is stated here for every family.

| Method | Reads | Returns |
|--------|-------|---------|
| `list_unused_indexes(schema=None)` | `pg_stat_user_indexes` where `idx_scan = 0` | Schema, table and index address |
| `list_table_usage_stats(schema=None)` | `pg_stat_user_tables` | Sequential and index scans, insert/update/delete counts, live-row estimate |
| `list_runtime_metrics(schema=None)` | `pg_stat_activity` | One row per backend process |
| `list_index_usage_stats(schema=None)` | `pg_stat_user_indexes`, `pg_relation_size()` | Scans, rows read, rows fetched, index bytes |
| `list_object_dependencies(object_name=None, schema=None)` | `pg_depend`, `pg_class`, `pg_namespace` | Dependency edges |
| `get_ddl_statement(object_name, object_type=None, schema=None)` | `pg_class`, `pg_namespace`; `pg_get_expr` / `pg_get_viewdef` recover expressions | Object address and kind; no text today, and an assembly is marked `native=False` |

### `list_unused_indexes`

PostgreSQL ships no vendor view for unused indexes, so the listing is computed
from the counters: `pg_stat_user_indexes` rows with `idx_scan = 0` in the
requested schema, joined to `pg_namespace` for the schema name. The projection
is the address — `schema_name`, `table_name`, `index_name` — because the address
is what the family's common shape carries, and the counter that selected the row
is zero by definition.

A zero counter means no index scan since the statistics were last reset. That is
PostgreSQL's tense for this family and a weaker claim than the word "unused"
suggests: an index untouched since a crash, or since a `pg_stat_reset()`, reports
zero. The answer is "worth reviewing", never "safe to drop". The listing only
covers indexes the statistics view has a row for; an index it does not list is
not thereby proven used.

### `list_table_usage_stats`

The listing reads `pg_stat_user_tables` for the requested schema, one row per
table the collector has seen. It projects `seq_scan`, `idx_scan`, `n_tup_ins`,
`n_tup_upd`, `n_tup_del` and `n_live_tup`. The view's other facts — dead tuples,
the last-access times — are real but are not projected: they have no common slot
in `TableUsageStats`, and where they go is the backend reader's decision rather
than the common shape's. A table with no statistics row is absent from the
listing, which is the honest scope rather than a zero-filled row. A zero in
`seq_scan` and a zero in `n_tup_ins` support different conclusions, and neither
survives being read as "no activity at all".

### `list_runtime_metrics`

The listing reads `pg_stat_activity` and answers one row per backend process.
It takes no table name: a runtime measurement is about the server, and a table
filter would discard rows that were never about a table. The projection is
`application_name` as `name`, `state` as `value`, `wait_event_type` as `kind`,
`wait_event` as `description`, and `backend_start` as `source`. `source` dates
the session the row describes rather than labelling a counter window; the counter
window is the statistics window stated above, the same for every row.
`pg_stat_statements` is deliberately not read: it is an extension, and answering
from a catalogue that is only sometimes installed would make the family depend
on a module rather than on the engine.

### `list_index_usage_stats`

The same `pg_stat_user_indexes` view, read for how much rather than whether. The
projection is `idx_scan` (scans), `idx_tup_read` (rows read), `idx_tup_fetch`
(rows fetched) and `pg_relation_size(indexrelid)` (index bytes) in one row. The
size call is why "how much use" and "how much space" land together instead of
two queries a caller has to join. Every counter is since the statistics were last
reset; a zero here is a measurement, not an absence. The listing covers the
indexes the statistics view has rows for.

### `list_object_dependencies`

The graph is read from `pg_depend`, joined to `pg_class` on `refobjid` and to
`pg_namespace` for the schema. Each row describes an edge in which a relation is
the referenced object; `object_name`, when given, narrows the listing to that
relation's edges — the "what breaks if I drop this" question. The projection is
`schema_name`, the referenced relation's `relname` as `referenced`, `deptype` as
`kind`, and `classid` as `referenced_type`. `action` stays unset: PostgreSQL has
no per-edge action column, and a dependant's behaviour on drop is a property of
its class, so the formatter does not guess one from `deptype`. There is no
`deptype` filter either; the kind is reported so the reader can decide which
edges to act on.

### `get_ddl_statement`

`supports_ddl_extraction` answers `True` and a statement renders, but the
statement this backend renders is an address, not text. It selects
`pg_class.relname`, `pg_namespace.nspname` and `pg_class.relkind` for the named
relation, and projects no `ddl` column. The parser skips a row carrying no text,
so `get_ddl_statement` returns `None` for a table that exists: "cannot reproduce
this", not "no such table". `supports_ddl_extraction_native` answers `False`, so
a caller who needs the server's own text knows in advance that any assembly here
would be a reconstruction; `pg_get_expr` and `pg_get_viewdef` can recover
expressions, but a faithful `CREATE TABLE` would also need storage parameters,
tablespace and per-column clauses the catalogue keeps only in parts. The CLI
reports `Error: no DDL available for '<name>'` and exits `1`; it does not print
an empty row, because a row with no text would read as an answer.

## Column Data Profiles

The three data families read `pg_stats`, the per-column statistics `ANALYZE`
leaves, not the rows themselves. `pg_stats` holds one row per analysed column,
so the profile costs one catalogue query however large the table is — a catalogue
read rather than a pass over the rows, after the cheap column listing core reads
first. The price is that every figure is an estimate of the last `ANALYZE`, which
is why the answer carries its source and the time it was collected.

| Method | Reads | Returns |
|--------|-------|---------|
| `list_column_data_profiles(table_name, schema=None)` | One `pg_stats` row per analysed column | Null fraction, distinct count, extremes, row estimate, correlation, and the kept value list |
| `sample_column_values(table_name, column_name, limit=100, schema=None)` | `pg_stats.most_common_vals` and `most_common_freqs` | Value and frequency pairs |
| `estimate_join_coverage(left_table, left_column, right_table, right_column, schema=None)` | The rows of both tables | Two coverage fractions |

### What `pg_stats` Will Not Give

`pg_stats` declares `histogram_bounds` and `most_common_vals` as `anyarray`, the
pseudo-type that stands for "an array of whatever this column's type is", and
keeps `most_common_freqs` beside them. PostgreSQL refuses to subscript an
`anyarray` column (`cannot subscript type anyarray`) and defines no `MIN`/`MAX`
aggregate over one, because a type without a btree opclass has no aggregate.
`array_to_json` is declared over `anyarray`, so it is the one call that accepts
the columns as they are; this backend reads all three arrays through it. The
cost is that a bound arrives as a JSON scalar: a number stays a number, while a
date, a timestamp or a UUID reads as its ISO text. A typed endpoint is not
obtainable from `pg_stats` without knowing the column's type at plan time, which
is a limit of the catalogue rather than a choice of the library. Everything else
on the profile — `null_frac`, `n_distinct`, `correlation`, `reltuples` — is
native.

### The Encodings the Parser Converts

`n_distinct` is signed. A negative value is not a count but the fraction of the
rows that are distinct — ANALYZE writes it that way when the column's cardinality
grows with the table — so the parser multiplies it by `reltuples` to produce the
count. A non-negative value is already a count and is carried through. Without
the conversion a unique column on a million-row table would report `-1` rather
than a million.

`-1` is "unknown", not minus one of anything. `null_frac` is `-1` on a column
ANALYZE has not measured since the statistics were reset, and `reltuples` is
`-1` for a relation the planner has never scanned; the parser turns both into
`None`. A caller that reads a negative fraction as a fraction, or a negative
row count as a count, would divide by a number that means "nobody looked".

`histogram_bounds` excludes the values ANALYZE kept as most-common. Its first
and last elements are therefore the extremes of what is left, not of the column:
a status column whose minimum is also its most common value has that value in
`most_common_vals`, not at the histogram's edge. The parser merges the kept
`most_common_vals` back in before calling the endpoints `min_value` and
`max_value`. Only like kinds are reconciled — two numbers, or two strings, the
latter covering text and the ISO spellings of dates and UUIDs — because a JSON
string and a JSON number have no meaningful order between them.

`pg_stats` has a row only for a column ANALYZE has seen. A column with no
statistics row is simply not in the profile list, so the answer is one entry per
analysed column, not one entry per declared column; read it beside
`list_columns`, not in place of it.

### The Source This Backend Reports

Every profile this backend returns carries `ProfileSource.ESTIMATE` when
`pg_stat_all_tables.last_analyze` or `last_autoanalyze` is recorded, and
`ProfileSource.UNKNOWN` when neither is. Both figures are estimates either way;
the rule is that an estimate carries the time it was collected, and `UNKNOWN` is
how a profile says its vintage is not known.

The timestamp is often absent for a reason worth knowing: PostgreSQL's
statistics collector writes `last_analyze` asynchronously, so a profile read
immediately after `ANALYZE` usually finds no timestamp yet. A `pg_stat_reset()`
also erases the time while leaving the statistics in place. Neither is an error,
and neither licenses inventing a date. The join from `pg_stats` to
`pg_stat_all_tables` is a LEFT join for this reason: a table the collector has
not seen still yields its `pg_stats` rows when it has any, with an unknown
vintage.

A table that was never analysed has no `pg_stats` row, so it answers with no
rows at all. That is "not measured" as an absence, not a profile with zeros in
its slots.

```python
from rhosocial.activerecord.backend.introspection.types import ProfileSource

profiles = backend.introspector.list_column_data_profiles("orders")
for profile in profiles:
    print(
        profile.column_name,
        profile.null_fraction,
        profile.distinct_count,
        profile.min_value,
        profile.max_value,
        profile.row_count,
        profile.source,
        profile.collected_at,
    )
    if profile.source is ProfileSource.ESTIMATE:
        print("  measured at", profile.collected_at)
```

### Sampling a Column's Values

`sample_column_values` projects the two arrays ANALYZE keeps in lockstep:
`most_common_vals` and `most_common_freqs`. The parser pairs them by position, so
the i-th frequency belongs to the i-th value, and a missing frequency stays
`None`. The arrays are kept side by side rather than unnested in SQL because
their element types differ; keeping them apart preserves the values in the
column's own types. ANALYZE keeps at most `statistics_target` values (100 by
default), so a low-cardinality column's listing is usually complete while a
high-cardinality one is a sample of the head; fewer values than requested is the
normal answer, not a truncated one. This backend slices to the requested `limit`
after reading the catalogue, since unnesting two arrays to pair and limit them
in SQL would cost more than the slice. `frequency` is populated; `count` stays
`None`, because `pg_stats` reports shares rather than row counts. NULL is a
legitimate member of the domain and is kept.

### Measuring Join Coverage

PostgreSQL answers `supports_join_coverage`, and the statement it renders is the
only family here that names two user tables rather than one catalogue relation;
both identifiers are quoted by the dialect. The row carries two fractions,
`left_coverage` and `right_coverage`, each computed as the share of one column's
distinct non-null values that also occurs in the other column's distinct non-null
values. Distinct on both sides is what makes the figure a coverage of values
rather than of rows: a key repeated a thousand times that matches once would
otherwise read as well covered.

NULLs are excluded from the numerators and the denominators alike. In the
numerator the exclusion is load-bearing, because `SELECT DISTINCT` treats a NULL
as one value; in the divisor `COUNT(DISTINCT ...)` skips NULLs on its own, but
the statement says so explicitly because the share is of the values that could
join. `NULLIF(COUNT(DISTINCT ...), 0)` then makes a column with no value at all
answer NULL rather than `0.0`: "nothing matched" and "nothing to match" stay
apart. Zero overlap is a measurement and comes back as `0.0`. The `* 1.0` in
each fraction is also load-bearing: PostgreSQL divides integers by truncation,
so `COUNT(*) / COUNT(...)` would answer zero for every share below one.

Both fractions are reported because the relation is not symmetric. A left
coverage of `1.0` beside a right coverage of `0.2` says every tested value
exists on the other side while most of that other domain is unaccounted for;
one similarity score would average the two and describe neither. Collations are
not reconciled: values the database would treat as equal under one collation may
be counted as distinct here, so the fractions are a lower bound across
mismatched collations, never an upper one.

The cost is a double scan plus an intersection of the two distinct sets, which
is why the probe answers `True` on the capability and the call is never implicit.
Profiling a table does not measure any join coverage, listing tables does not,
and no schema pass asks for it on the way past. `estimate_join_coverage` returns
`None` when the row carries no fraction at all; the CLI reports that and exits
`1` rather than printing a row of nulls that would read as a measured zero.

```python
coverage = backend.introspector.estimate_join_coverage(
    "orders", "buyer_id",
    "users", "id",
)
if coverage is None:
    print("no measurement")
else:
    print(coverage.left_coverage, coverage.right_coverage)
```

## Async API

The async backend provides identical introspection methods with the same names as the sync version:

```python
from rhosocial.activerecord.backend.impl.postgres.backend.async_backend import AsyncPostgresBackend
backend = AsyncPostgresBackend(
    host="localhost",
    port=5432,
    database="mydb",
    user="postgres",
    password="password"
)
await backend.connect()

# Async introspection methods (same names as sync version)
db_info = await backend.introspector.get_database_info()
tables = await backend.introspector.list_tables()
columns = await backend.introspector.list_columns("users")
indexes = await backend.introspector.list_indexes("users")
```

## Cache Management

Introspection results are cached by default for performance:

```python
# Clear all introspection cache
backend.introspector.clear_cache()

# Invalidate specific scope
from rhosocial.activerecord.backend.introspection.types import IntrospectionScope

# Invalidate all table-related cache
backend.introspector.invalidate_cache(scope=IntrospectionScope.TABLE)

# Invalidate cache for a specific table
backend.introspector.invalidate_cache(
    scope=IntrospectionScope.TABLE,
    name="users"
)

# Invalidate cache for a specific schema
backend.introspector.invalidate_cache(
    scope=IntrospectionScope.TABLE,
    name="users",
    table_name="users"  # Used to qualify schema
)
```

## Version Compatibility

Introspection behavior differences across PostgreSQL versions:

| Feature | PostgreSQL 12+ | PostgreSQL 11 and below |
|---------|----------------|------------------------|
| Materialized view info | Full support | Basic support |
| Partitioned table info | Full support | Limited support |
| Generated columns | Supported | Not supported |
| Stored procedure triggers | Supported | Not supported |

## Best Practices

### 1. Use Caching

Introspection operations involve complex `pg_catalog` queries. Leverage caching:

```python
# First query caches the result
tables = backend.introspector.list_tables()

# Subsequent queries return from cache
tables_again = backend.introspector.list_tables()

# Only clear cache when table structure changes
backend.introspector.invalidate_cache(scope=IntrospectionScope.TABLE, name="users")
```

### 2. Specify Schema

In multi-schema environments, explicitly specify the schema:

```python
# Explicitly specify schema to avoid ambiguity
table_info = backend.introspector.get_table_info("users", schema="auth")
```

### 3. Leverage Index Type Information

Optimize queries based on index types:

```python
from rhosocial.activerecord.backend.introspection.types import IndexType

indexes = backend.introspector.list_indexes("documents")
for idx in indexes:
    if idx.index_type == IndexType.GIN:
        # GIN indexes are suitable for JSONB or full-text search queries
        print(f"GIN index available: {idx.name}")
```

### 4. Concurrent Queries in Async Environment

```python
async def get_schema_info(backend):
    # Concurrently fetch columns for multiple tables
    import asyncio
    tables = await backend.introspector.list_tables()
    tasks = [
        backend.introspector.list_columns(table.name)
        for table in tables
    ]
    all_columns = await asyncio.gather(*tasks)
    return dict(zip([t.name for t in tables], all_columns))
```

## API Reference

### Core Methods

| Method | Description | Parameters |
|--------|-------------|------------|
| `get_database_info()` | Get database information | None |
| `list_tables()` | List tables | `include_system`, `table_type`, `schema` |
| `get_table_info(name)` | Get table details | `name`, `schema` |
| `table_exists(name)` | Check table exists | `name`, `schema` |
| `list_columns(table_name)` | List columns | `table_name`, `schema` |
| `get_column_info(table_name, column_name)` | Get column details | `table_name`, `column_name`, `schema` |
| `get_primary_key(table_name)` | Get primary key | `table_name`, `schema` |
| `list_indexes(table_name)` | List indexes | `table_name`, `schema` |
| `get_index_info(table_name, index_name)` | Get index details | `table_name`, `index_name`, `schema` |
| `index_exists(table_name, index_name)` | Check index exists | `table_name`, `index_name`, `schema` |
| `list_foreign_keys(table_name)` | List foreign keys | `table_name`, `schema` |
| `list_views()` | List views | `schema` |
| `get_view_info(name)` | Get view details | `name`, `schema` |
| `view_exists(name)` | Check view exists | `name`, `schema` |
| `list_triggers(table_name)` | List triggers | `table_name`, `schema` |
| `get_trigger_info(table_name, trigger_name)` | Get trigger details | `table_name`, `trigger_name`, `schema` |
| `clear_cache()` | Clear cache | None |
| `invalidate_cache(scope, ...)` | Invalidate cache | `scope`, `name`, `table_name` |

### PostgreSQL-Specific Methods

| Method | Description | Parameters |
|--------|-------------|------------|
| `list_materialized_views()` | List materialized views | `schema`, `include_system` |
| `get_materialized_view_info(name)` | Get one materialized view | `name`, `schema` |
| `materialized_view_exists(name)` | Check a materialized view exists | `name`, `schema` |
| `list_sequences()` | List sequences | `schema` |
| `get_sequence_info(name)` | Get one sequence | `name`, `schema` |
| `sequence_exists(name)` | Check a sequence exists | `name`, `schema` |
| `list_extensions()` | List installed extensions | None |
| `get_extension_info(name)` | Get one installed extension | `name` |
| `extension_exists(name)` | Check an extension is installed | `name` |
| `list_unused_indexes()` | Indexes with no scan since the statistics reset | `schema` |
| `list_table_usage_stats()` | Per-table usage counters | `schema` |
| `list_runtime_metrics()` | One row per backend process | `schema` |
| `list_index_usage_stats()` | Per-index counters and size | `schema` |
| `list_object_dependencies()` | Dependency edges, or one object's | `object_name`, `schema` |
| `get_ddl_statement(name)` | The DDL text, or `None` on this backend | `name`, `object_type`, `schema` |
| `list_column_data_profiles(table_name)` | Per-column profile from `pg_stats` | `table_name`, `schema` |
| `sample_column_values(table_name, column_name)` | Kept values and frequencies | `table_name`, `column_name`, `limit`, `schema` |
| `estimate_join_coverage(...)` | Two coverage fractions | `left_table`, `left_column`, `right_table`, `right_column`, `schema` |

The three data methods are also declared on core's introspector and use the same
names; the PostgreSQL reader supplies the parsing, and the async twin awaits the
same names.

## Command Line Introspection

The PostgreSQL backend provides command-line introspection commands to query database metadata without writing code.

### Basic Usage

```bash
# List all tables
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --host localhost --port 5432 --database mydb --user postgres --password

# List all views
python -m rhosocial.activerecord.backend.impl.postgres introspect views \
  --database mydb --user postgres

# Get database information
python -m rhosocial.activerecord.backend.impl.postgres introspect database \
  --database mydb --user postgres

# Include system tables
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --include-system
```

### Connection Parameters

Supports configuration via command-line arguments or environment variables:

| Parameter | Environment Variable | Description |
|-----------|---------------------|-------------|
| `--host` | `POSTGRES_HOST` | Database host (default: localhost) |
| `--port` | `POSTGRES_PORT` | Database port (default: 5432) |
| `--database` | `POSTGRES_DATABASE` | Database name (required) |
| `--user` | `POSTGRES_USER` | Username (default: postgres) |
| `--password` | `POSTGRES_PASSWORD` | Password |

### Querying Table Details

```bash
# Query only column information
python -m rhosocial.activerecord.backend.impl.postgres introspect columns users \
  --database mydb --user postgres

# Query only index information
python -m rhosocial.activerecord.backend.impl.postgres introspect indexes users \
  --database mydb --user postgres

# Query only foreign key information
python -m rhosocial.activerecord.backend.impl.postgres introspect foreign-keys posts \
  --database mydb --user postgres

# Query triggers
python -m rhosocial.activerecord.backend.impl.postgres introspect triggers \
  --database mydb --user postgres

# Query triggers for a specific table
python -m rhosocial.activerecord.backend.impl.postgres introspect triggers users \
  --database mydb --user postgres
```

### Schema Support

PostgreSQL supports schema parameter:

```bash
# Specify schema to query tables
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --schema auth
```

### Querying Operational and Data Families

The six operational families and the three data families are their own types:

```bash
# Indexes no query has used since the statistics were reset
python -m rhosocial.activerecord.backend.impl.postgres introspect unused-indexes \
  --database mydb --user postgres

# Per-table usage counters and per-index counters
python -m rhosocial.activerecord.backend.impl.postgres introspect table-stats \
  --database mydb --user postgres
python -m rhosocial.activerecord.backend.impl.postgres introspect index-stats \
  --database mydb --user postgres

# What the server has been doing
python -m rhosocial.activerecord.backend.impl.postgres introspect runtime \
  --database mydb --user postgres

# The dependency graph, or one object's edges
python -m rhosocial.activerecord.backend.impl.postgres introspect dependencies orders \
  --database mydb --user postgres

# The DDL that recreates a table (this backend answers "no DDL available")
python -m rhosocial.activerecord.backend.impl.postgres introspect ddl orders \
  --database mydb --user postgres

# Profiles and samples from pg_stats
python -m rhosocial.activerecord.backend.impl.postgres introspect column-profiles orders \
  --database mydb --user postgres
python -m rhosocial.activerecord.backend.impl.postgres introspect value-samples orders \
  --column status --database mydb --user postgres

# Overlap between two columns' value domains
python -m rhosocial.activerecord.backend.impl.postgres introspect join-coverage orders \
  --column buyer_id --right-table users --right-column id \
  --database mydb --user postgres

# One table's storage split
python -m rhosocial.activerecord.backend.impl.postgres introspect table-storage orders \
  --database mydb --user postgres
```

### Introspection Types

| Type | Description | NAME |
|------|-------------|------|
| `tables` | Every relation in the schema: base tables, views, materialized views, foreign tables and partitioned tables | Refused |
| `views` | List ordinary views, or one by name | Optional: a view name |
| `columns` | One table's columns | Required: a table name |
| `indexes` | One table's indexes | Required: a table name |
| `foreign-keys` | One table's foreign keys | Required: a table name |
| `triggers` | Triggers, optionally narrowed to one table | Optional: a table name |
| `database` | Server version, charset, and the databases visible to this role | Refused |
| `extensions` | Installed extensions, or one by name | Optional: an extension name |
| `materialized-views` | Materialized views, or one by name | Optional: a view name |
| `sequences` | Sequences, or one by name | Optional: a sequence name |
| `table-storage` | One table's storage split and row estimate | Required: a table name |
| `unused-indexes` | Indexes with `idx_scan = 0` since the statistics were reset | Refused |
| `table-stats` | Per-table usage counters | Refused |
| `runtime` | One row per backend process from `pg_stat_activity` | Refused |
| `index-stats` | Per-index counters and size | Refused |
| `dependencies` | Dependency edges, or one object's | Optional: an object name |
| `ddl` | The DDL that recreates one table | Required: a table name |
| `column-profiles` | Per-column profiles from `pg_stats` | Required: a table name |
| `value-samples` | A column's kept values and frequencies | Required: a table name; `--column` required |
| `join-coverage` | Two columns' value-domain overlap | Required: the left table; `--column`, `--right-table`, `--right-column` required |

Ten rows here are new relative to the ten types this page listed before; nine of
them are the families above. `table-storage` asks the declaration layer for one
table's storage split; `unused-indexes`, `table-stats`, `runtime`, `index-stats`,
`dependencies` and `ddl` are the six operational families; `column-profiles`,
`value-samples` and `join-coverage` are the three data families. Each is offered
because the probe switch named in `_TYPE_CAPABILITIES` answers `True` on the
adapted server; a type whose switch is `False` exits `2` with "not supported by
this server" rather than returning an empty list.

`NAME` selects the object the type answers for, and each type has exactly one
answer to whether it takes one. `columns`, `indexes`, `foreign-keys`,
`table-storage`, `ddl`, `column-profiles`, `value-samples` and `join-coverage`
name the table they are about and require it — for `join-coverage` that is the
left table. `triggers` accepts a table name, `dependencies` accepts an object
name, and `views`, `materialized-views`, `sequences` and `extensions` accept an
object name; each lists its whole family when it gets none. `tables`, `database`,
`unused-indexes`, `table-stats`, `runtime` and `index-stats` take no name: a name
there is an error rather than a filter, because the counter listings answer for
the connection's scope rather than for one object.

Three types ask about a column or a second table on their own flags.
`value-samples` takes `--column`, and `join-coverage` takes `--column`,
`--right-table` and `--right-column`; a missing one is an incomplete invocation
and exits `1`. A flag given to a type that has no question for it exits `2`
rather than being ignored, the same rule NAME follows.

`ddl` is the one type that can legitimately report a failure on an existing
table: this backend's projection carries no DDL text, so `get_ddl_statement`
returns `None` and the command prints `Error: no DDL available for '<name>'`
and exits `1`. It does not print an empty row, because a row with no text would
read as an answer. `supports_ddl_extraction_native` is `False` here, so a caller
that needs the server's own text already knows the answer is an assembly, not
a retrieval.

A run exits `1` when it could not finish -- the connection failed, a query
failed, or the object a name asked for is not there -- and `2` when the request
cannot be served here: an unknown type, a name the type does not take, or a
family this server cannot answer.

### Output Formats

```bash
# Table format (default, requires rich library)
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres

# JSON format
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --output json

# CSV format
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --output csv

# TSV format
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --output tsv
```

### Using Async Backend

```bash
# Use --use-async flag to enable async mode
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --use-async
```

### Environment Variable Configuration

You can set connection parameters in the environment to simplify command-line calls:

```bash
# Set environment variables
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DATABASE=mydb
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=secret

# Use command directly
python -m rhosocial.activerecord.backend.impl.postgres introspect tables
```
