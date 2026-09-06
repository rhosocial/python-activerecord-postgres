# EXPLAIN Support

## Overview

The PostgreSQL backend provides comprehensive support for the `EXPLAIN` statement, enabling query performance analysis and execution plan inspection. This feature integrates with the ActiveRecord query builder through a unified API while exposing PostgreSQL-specific behaviors.

Key capabilities:
- Basic `EXPLAIN` for execution plan visualization
- `EXPLAIN ANALYZE` for actual runtime statistics (all modern versions)
- Multiple output formats: `TEXT`, `XML`, `JSON`, `YAML`
- PostgreSQL's bracketed option syntax (`EXPLAIN (ANALYZE, FORMAT JSON)`)
- Structured result parsing with `PostgresExplainResult`
- Index usage analysis helpers (`Seq Scan`, `Index Scan`, `Index Only Scan`)
- Full sync and async support

## Basic Usage

### Accessing EXPLAIN via Query Builder

```python
from rhosocial.activerecord.backend.expression.statements import ExplainFormat

# Simple EXPLAIN — returns PostgresExplainResult
result = User.query().explain().all()

# Inspect the generated SQL prefix
print(result.sql)  # EXPLAIN SELECT ...

# Print the execution time (seconds)
print(result.duration)
```

### Reading the Result Rows

```python
result = User.query().explain().all()

for row in result.rows:
    print(row.line)
```

Each row is a `PostgresExplainPlanLine` with a single field:

| Field | Type | Description |
|---|---|---|
| `line` | `str` | One line of the query plan text (e.g. `"Seq Scan on users  (cost=0.00..8.27 rows=1 width=...)"`). |

PostgreSQL returns each plan node as an indented text line in the `"QUERY PLAN"` column. All lines together form the full execution plan tree.

## Output Formats

PostgreSQL supports four output formats, all available on any modern server version:

### TEXT Format (Default)

```python
# Default — no FORMAT option emitted
result = User.query().explain().all()
# Generates: EXPLAIN SELECT ...
```

### JSON Format

```python
result = User.query().explain(format=ExplainFormat.JSON).all()
# Generates: EXPLAIN (FORMAT JSON) SELECT ...
```

### XML Format

```python
result = User.query().explain(format=ExplainFormat.XML).all()
# Generates: EXPLAIN (FORMAT XML) SELECT ...
```

### YAML Format

```python
result = User.query().explain(format=ExplainFormat.YAML).all()
# Generates: EXPLAIN (FORMAT YAML) SELECT ...
```

## EXPLAIN ANALYZE

`EXPLAIN ANALYZE` executes the query and returns actual runtime statistics alongside cost estimates. Use it to compare estimated vs. actual row counts and timing.

```python
# Basic ANALYZE
result = User.query().explain(analyze=True).all()
# Generates: EXPLAIN (ANALYZE) SELECT ...

# ANALYZE with JSON format
result = User.query().explain(analyze=True, format=ExplainFormat.JSON).all()
# Generates: EXPLAIN (ANALYZE, FORMAT JSON) SELECT ...

# ANALYZE with YAML format
result = User.query().explain(analyze=True, format=ExplainFormat.YAML).all()
# Generates: EXPLAIN (ANALYZE, FORMAT YAML) SELECT ...
```

**Important Notes:**
- `EXPLAIN ANALYZE` **executes the query** against the database. Avoid using it on write statements in production without understanding the side effects.
- Supported on all modern PostgreSQL versions (no minimum version constraint within the supported range).

## SQL Syntax Generated

PostgreSQL uses a parenthesized option syntax, distinct from MySQL's flat syntax:

```
EXPLAIN [ ( option [, ...] ) ] statement
```

Options are comma-separated inside parentheses:

```python
# No options
# → EXPLAIN SELECT ...

# analyze only
# → EXPLAIN (ANALYZE) SELECT ...

# format only
# → EXPLAIN (FORMAT JSON) SELECT ...

# both analyze and format
# → EXPLAIN (ANALYZE, FORMAT JSON) SELECT ...
```

The `ANALYZE` option always appears before `FORMAT` in the option list.

## Index Usage Analysis

`PostgresExplainResult` provides helpers for a quick index usage assessment based on text matching across all plan lines:

```python
result = User.query().where(User.c.email == "alice@example.com").explain().all()

usage = result.analyze_index_usage()
print(usage)  # "full_scan" | "index_with_lookup" | "covering_index" | "unknown"

# Convenience properties
if result.is_full_scan:
    print("Warning: sequential scan detected")

if result.is_covering_index:
    print("Optimal: index only scan in use")

if result.is_index_used:
    print("An index is being used")
```

### Index Usage Rules

The analysis concatenates all plan lines and performs case-insensitive text matching:

| Matched keyword (case-insensitive) | `analyze_index_usage()` return |
|---|---|
| `"INDEX ONLY SCAN"` (checked first) | `"covering_index"` |
| `"INDEX SCAN"`, `"BITMAP INDEX SCAN"`, or `"BITMAP HEAP SCAN"` | `"index_with_lookup"` |
| `"SEQ SCAN"` | `"full_scan"` |
| None of the above | `"unknown"` |

**Note:** `"INDEX ONLY SCAN"` is checked before `"INDEX SCAN"` to avoid misclassification.

## Async API

The async backend provides the same interface. Use `await` for the terminal method:

```python
async def analyze_queries():
    # Simple async EXPLAIN
    result = await User.query().explain().all_async()

    # ANALYZE with JSON format
    result = await User.query().explain(
        analyze=True,
        format=ExplainFormat.JSON
    ).all_async()

    for row in result.rows:
        print(row.line)
```

## Checking Format Support at Runtime

Use `dialect` methods to guard option availability:

```python
dialect = backend.dialect

# Check ANALYZE support (always True for PostgreSQL)
if dialect.supports_explain_analyze():
    result = User.query().explain(analyze=True).all()

# Check a specific format (all four formats always supported)
if dialect.supports_explain_format("JSON"):
    result = User.query().explain(format=ExplainFormat.JSON).all()
```

## Combining EXPLAIN with Complex Queries

EXPLAIN works with the full query builder chain:

```python
# JOIN query
result = (
    User.query()
    .join(Order, User.c.id == Order.c.user_id)
    .where(User.c.status == "active")
    .explain(format=ExplainFormat.JSON)
    .all()
)

# GROUP BY / aggregate
result = (
    Order.query()
    .group_by(Order.c.user_id)
    .explain(analyze=True)
    .count(Order.c.id)
)

# Subquery
result = (
    User.query()
    .where(User.c.id.in_(Order.query().select(Order.c.user_id)))
    .explain()
    .all()
)
```

## Comparison with MySQL EXPLAIN

Both backends share the same query builder API but differ in SQL syntax and result structure:

| Feature | PostgreSQL | MySQL |
|---|---|---|
| SQL syntax | `EXPLAIN (ANALYZE, FORMAT JSON)` | `EXPLAIN ANALYZE FORMAT=JSON` |
| Option delimiter | Comma-separated, parenthesized | Space-separated, no parentheses |
| Supported formats | TEXT, XML, JSON, YAML | TEXT, JSON (5.6.5+), TREE (8.0.16+) |
| ANALYZE support | All modern versions | 8.0.18+ |
| Result row type | `PostgresExplainPlanLine` (1 field) | `MySQLExplainRow` (12 fields) |
| Covering index indicator | `"Index Only Scan"` in plan text | `"Using index"` in `extra` column |

## Version Compatibility

| Feature | Minimum PostgreSQL Version |
|---|---|
| Basic `EXPLAIN` | All supported versions |
| `EXPLAIN FORMAT=TEXT/XML/JSON/YAML` | All supported versions |
| `EXPLAIN ANALYZE` | All supported versions |
| `EXPLAIN (ANALYZE, FORMAT JSON)` | All supported versions |

The PostgreSQL backend targets PostgreSQL 9.6 and later; all EXPLAIN features described here are available on 9.6+.

## API Reference

### `ExplainOptions` Fields Used by PostgreSQL

| Field | Type | Default | Description |
|---|---|---|---|
| `analyze` | `bool` | `False` | Emit `ANALYZE` option |
| `format` | `Optional[ExplainFormat]` | `None` | Output format (`TEXT`/`XML`/`JSON`/`YAML`) |

Other `ExplainOptions` fields (such as `verbose`, `buffers`, `timing`, `settings`) are accepted but not yet emitted by the current PostgreSQL dialect implementation.

### `PostgresExplainResult` Methods

| Method / Property | Description |
|---|---|
| `rows` | `List[PostgresExplainPlanLine]` — parsed plan lines |
| `sql` | The full SQL string that was explained |
| `duration` | Query execution time in seconds |
| `raw_rows` | Raw rows as returned by psycopg (list of dicts with `"QUERY PLAN"` key) |
| `analyze_index_usage()` | Returns `"full_scan"`, `"index_with_lookup"`, `"covering_index"`, or `"unknown"` |
| `is_full_scan` | `True` if `analyze_index_usage() == "full_scan"` |
| `is_index_used` | `True` if any index is used |
| `is_covering_index` | `True` if an index-only scan is used |

### `PostgresDialect` Methods

| Method | Return | Description |
|---|---|---|
| `supports_explain_analyze()` | `bool` | Always `True` for PostgreSQL |
| `supports_explain_format(fmt)` | `bool` | `True` for `TEXT`, `XML`, `JSON`, `YAML` |
| `format_explain_statement(expr)` | `str` | Builds `EXPLAIN [(ANALYZE, FORMAT X)]` prefix |

## Best Practices

- **Use JSON format for programmatic analysis.** `FORMAT JSON` produces a structured plan tree that is easier to parse than plain text and provides the most detail.
- **Reserve `EXPLAIN ANALYZE` for development.** The statement executes the query, so use it only when you need actual row counts and timing figures.
- **Leverage all four formats for different audiences.** Use `TEXT` for human inspection, `JSON`/`YAML` for tooling, and `XML` for XML-based reporting pipelines.
- **Combine with index analysis.** Use `result.is_full_scan` as a quick smoke test in integration tests to detect inadvertent sequential scans.

💡 *AI Prompt: Explain how to use EXPLAIN ANALYZE with JSON format on PostgreSQL to diagnose slow queries in rhosocial-activerecord.*
