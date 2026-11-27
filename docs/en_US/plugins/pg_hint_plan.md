# pg_hint_plan

`pg_hint_plan` lets you influence PostgreSQL's planner with hint phrases in a special SQL comment, e.g. `/*+ SeqScan(t) */`. It is useful when cost estimates disagree with data properties the planner cannot see (column correlation, skewed distributions).

💡 *AI Prompt:* "When is it appropriate to pin a plan with pg_hint_plan instead of changing statistics or indexes?"

## Availability

The `postgres-exts` images ship `pg_hint_plan` 1.8.1 on PG12–18. Enable it per database:

```sql
CREATE EXTENSION pg_hint_plan;
```

**Important:** `CREATE EXTENSION` alone does not install the planner hook. Unless the server lists `pg_hint_plan` in `shared_preload_libraries`, load it once per session:

```python
from rhosocial.activerecord.backend.impl.postgres.functions.pg_hint_plan import load_pg_hint_plan

backend.execute(*load_pg_hint_plan(dialect).to_sql())
```

## Building hints

A pg_hint_plan hint is a comment carried by **one statement**, not a statement of its own. The builders return plain dictionary payloads (`HintPayload`), never SQL strings and never expression nodes; the dialect formatter is the only place that renders the comment.

Hints are PostgreSQL-specific, so they do not live on core's statements. The backend **derives** its own statement classes, each adding exactly one keyword-only parameter, `hints` (case B of the framework's expression/format rules: `E_c = derive(E_core, +params_c)`, with the base parameters untouched). Each derived class inherits its base's `format_method`, so no new expression/formatter pair appears; the PostgreSQL formatter handles both the generic and the derived expression — a generic statement renders exactly as before, and only a derived instance carrying hints is prefixed.

Eight statements carry hints on a PostgreSQL statement class:

| PostgreSQL class | Statement | Module |
|---|---|---|
| `PostgresQueryStatement` | `SELECT` | `expression/hinted_statements.py` |
| `PostgresInsertStatement` | `INSERT` | `expression/hinted_statements.py` |
| `PostgresUpdateStatement` | `UPDATE` | `expression/hinted_statements.py` |
| `PostgresDeleteStatement` | `DELETE` | `expression/hinted_statements.py` |
| `PostgresExplainStatement` | `EXPLAIN` | `expression/hinted_statements.py` |
| `PostgresCreateTableAsStatement` | `CREATE TABLE ... AS` | `expression/hinted_statements.py` |
| `PostgresCreateMaterializedViewStatement` | `CREATE MATERIALIZED VIEW ... AS` | `expression/ddl/mv.py` |
| `PostgresRefreshMaterializedViewStatement` | `REFRESH MATERIALIZED VIEW` | `expression/ddl/mv.py` |

The last two were already PostgreSQL-derived before hint support and were extended in place; the other six are the backend's counterparts of core's statements.

Pass the payloads through the statement's `hints=` keyword:

```python
from rhosocial.activerecord.backend.impl.postgres.expression.hinted_statements import (
    PostgresQueryStatement,
)
from rhosocial.activerecord.backend.impl.postgres.functions.pg_hint_plan import (
    hint_seq_scan, hint_hash_join, hint_leading, hint_rows,
    hint_parallel, hint_set,
)
from rhosocial.activerecord.backend.impl.postgres.expression.hint import HintGroup

query = PostgresQueryStatement(
    dialect,
    select=[...],
    from_=...,
    hints=(
        hint_leading(dialect, HintGroup("a", "b"), "c"),   # Leading((a b) c)
        hint_hash_join(dialect, "a", "b"),
        hint_seq_scan(dialect, "a"),
    ),
)
sql, params = query.to_sql()
# /*+ Leading((a b) c) HashJoin(a b) SeqScan(a) */ SELECT ...
backend.execute(sql, params, options=ExecutionOptions(stmt_type=StatementType.DQL))
```

`PostgresExplainStatement` writes the comment before the `EXPLAIN` keyword, which is the position pg_hint_plan documents:

```python
from rhosocial.activerecord.backend.impl.postgres.expression.hinted_statements import (
    PostgresExplainStatement,
)

explained = PostgresExplainStatement(
    dialect, query, hints=(hint_seq_scan(dialect, "a"),)
)
sql, params = explained.to_sql()
# /*+ SeqScan(a) */ EXPLAIN SELECT ...
```

An `EXPLAIN` — generic or PostgreSQL — refuses a wrapped statement that already carries its own hints (two comment positions would be ambiguous).

The same `hints=` keyword is accepted by the three statements PostgreSQL plans when it runs them: `PostgresCreateTableAsStatement` (`CREATE TABLE ... AS`), `PostgresCreateMaterializedViewStatement` (`CREATE MATERIALIZED VIEW ... AS`) and `PostgresRefreshMaterializedViewStatement` (`REFRESH MATERIALIZED VIEW`). Measured against pg_hint_plan 1.8.1 on PG 18, each reports `used hint` for a head comment and picks a different plan:

```python
from rhosocial.activerecord.backend.impl.postgres.expression.hinted_statements import (
    PostgresCreateTableAsStatement,
)

ctas = PostgresCreateTableAsStatement(
    dialect, Table(dialect, "ct"), query, hints=(hint_seq_scan(dialect, "t"),)
)
sql, params = ctas.to_sql()
# /*+ SeqScan(t) */ CREATE TABLE "ct" AS SELECT ...
```

For those three, give the hint to the **statement**, not to the query it wraps: a second comment block is rejected by pg_hint_plan, and the formatter refuses a wrapped hinted query by name (`TypeError`, "a pg_hint_plan hint belongs to the outermost statement"). `CREATE VIEW` has no hinted class at all: it is not planned at creation time, so pg_hint_plan ignores the comment (measured: no log line); a hinted query inside its body is refused rather than silently made inert. Hint the query that reads the view instead.

A hint payload is a plain dictionary with every key present (`None`/`()` where a field does not apply; `make_hint_payload()` builds one), so it survives artifact serialization; it is not an expression node and never renders on its own. The derived statement is the node that owns it, and a dialect without the extension refuses a hinted statement with `UnsupportedFeatureError` instead of dropping the hints:

```python
dialect.supports_pg_hint_plan()   # True only when pg_hint_plan is available
```

The hint comment precedes the statement, so pass an explicit `stmt_type` when
executing a hinted query: the backend cannot infer the statement type from the
first word of the SQL.

### Hint vocabulary (26 kinds)

| Kind | Builder | Gate |
|---|---|---|
| `SeqScan` | `hint_seq_scan` | — |
| `TidScan` | `hint_tid_scan` | — |
| `IndexScan` | `hint_index_scan` | — |
| `IndexOnlyScan` | `hint_index_only_scan` | — |
| `BitmapScan` | `hint_bitmap_scan` | — |
| `IndexScanRegexp` | `hint_index_scan_regexp` | — |
| `IndexOnlyScanRegexp` | `hint_index_only_scan_regexp` | — |
| `BitmapScanRegexp` | `hint_bitmap_scan_regexp` | — |
| `NoSeqScan` | `hint_no_seq_scan` | — |
| `NoTidScan` | `hint_no_tid_scan` | — |
| `NoIndexScan` | `hint_no_index_scan` | — |
| `NoIndexOnlyScan` | `hint_no_index_only_scan` | — |
| `NoBitmapScan` | `hint_no_bitmap_scan` | — |
| `DisableIndex` | `hint_disable_index` | pg_hint_plan ≥ 1.8 |
| `NestLoop` | `hint_nest_loop` | — |
| `HashJoin` | `hint_hash_join` | — |
| `MergeJoin` | `hint_merge_join` | — |
| `NoNestLoop` | `hint_no_nest_loop` | — |
| `NoHashJoin` | `hint_no_hash_join` | — |
| `NoMergeJoin` | `hint_no_merge_join` | — |
| `Memoize` | `hint_memoize` | PostgreSQL ≥ 17 + pg_hint_plan ≥ 1.4 |
| `NoMemoize` | `hint_no_memoize` | PostgreSQL ≥ 17 + pg_hint_plan ≥ 1.4 |
| `Leading` | `hint_leading` | — |
| `Rows` | `hint_rows` | — |
| `Parallel` | `hint_parallel` | — |
| `Set` | `hint_set` | — |

Gates reflect the phrases that actually render and take effect (measured on PG 18.7 with pg_hint_plan 1.8.1, cross-checked against upstream release notes), not numbers copied from a doc table; `Memoize`/`NoMemoize` additionally require at least two relations in the hint.

A `*Regexp` builder takes POSIX patterns after the target and stores them in the payload's `indexes` field; `DisableIndex` takes index names there. Gated kinds build normally and are refused at render time with `UnsupportedFeatureError` (the error names the version in force and the required minimum).

## What a hint target is

A hint names a relation *the way the query does*: pg_hint_plan matches the hint text against the query's own spelling, so the builders take a relation reference — a `Table` (node, edge, materialized and foreign tables included), a `NamedRelationRef`, or a plain string for a pseudo name — and resolve it to one spelling. Measured rules:

- **An alias wins.** `FROM t1 a` matches `IndexScan(a)`; `IndexScan(t1)` is silently ignored.
- **A schema is never emitted.** `IndexScan(s2.t1)` does not even parse; a bare `IndexScan(t1)` still matches a reference written `FROM s2.t1`.
- **Same-named tables are ambiguous.** `FROM t1 a JOIN s2.t1 b` matches neither by name; aliases are the only disambiguation.
- **Quoting is exact.** A quoted name (`"Mixed"`, `"Odd Name"`) matches exactly; its lowercased spelling does not. The resolver quotes a name exactly when the object's `name_need_quote` is true, and never lowercases it.
- **Views cannot be hinted.** `IndexScan(v1)` is reported as an unused hint; hint the base tables of the query the view expands to instead.

```python
hint_index_scan(dialect, NamedRelationRef(dialect, Table(dialect, "orders"), alias="o"))
# /*+ IndexScan(o) */

hint_seq_scan(dialect, Table(dialect, "orders", schema_name="sales"))
# /*+ SeqScan("orders") */   -- the schema is never spelled in a hint
```

A plain string passes through unchanged: it is the escape hatch for a pseudo name (`*VALUES*`, `ANY_subquery`) or an alias the caller already knows. Resolution happens when the builder runs; a payload decoded from an artifact already holds the resolved strings and renders unchanged.

Identifiers and GUC values are validated with full matches; unsafe comment terminators and statement separators are rejected before rendering.

## Capability detection

```python
dialect.supports_pg_hint_plan()          # extension installed and version >= 1.3
dialect.get_extension_version("pg_hint_plan")
```

The version gates above use both `dialect.version` and `check_extension_feature("pg_hint_plan", "memoize")` for `Memoize`/`NoMemoize`, and `check_extension_feature("pg_hint_plan", "disable_index")` (via `get_extension_version`) for `DisableIndex`.

## See also

- **[unaccent](./unaccent.md)**: Accent-insensitive text normalization
- **[pg_trgm](./pg_trgm.md)**: Trigram similarity search
