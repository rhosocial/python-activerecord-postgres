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

Hint builders return **typed expression nodes**; they never assemble SQL strings. The dialect formatter is the only place that renders the comment.

```python
from rhosocial.activerecord.backend.impl.postgres.functions.pg_hint_plan import (
    hint_seq_scan, hint_index_scan, hint_hash_join, hint_leading,
    hint_rows, hint_parallel, hint_set, pg_hint_plan_hinted_query,
)
from rhosocial.activerecord.backend.impl.postgres.expression.hint import HintGroup

hints = [
    hint_leading(dialect, HintGroup("a", "b"), "c"),   # Leading((a b) c)
    hint_hash_join(dialect, "a", "b"),
    hint_seq_scan(dialect, "a"),
]
hinted = pg_hint_plan_hinted_query(dialect, query, hints)
sql, params = hinted.to_sql()
# /*+ Leading((a b) c) HashJoin(a b) SeqScan(a) */ SELECT ...
backend.execute(sql, params, options=ExecutionOptions(stmt_type=StatementType.DQL))
```

The hint comment precedes the statement, so pass an explicit `stmt_type` when
executing a hinted query: the backend cannot infer the statement type from the
first word of the SQL.

`dialect.build_hinted_query(query, hints)` is the equivalent dialect-level builder. Available builders: `hint_seq_scan`, `hint_tid_scan`, `hint_index_scan`, `hint_index_only_scan`, `hint_bitmap_scan`, `hint_no_seq_scan`, `hint_no_index_scan`, `hint_hash_join`, `hint_merge_join`, `hint_nest_loop`, `hint_leading`, `hint_rows`, `hint_parallel`, `hint_set`.

Identifiers and GUC values are validated with full matches; unsafe comment terminators and statement separators are rejected before rendering.

## Capability detection

```python
dialect.supports_pg_hint_plan()          # extension installed and version >= 1.3
dialect.get_extension_version("pg_hint_plan")
```

## See also

- **[unaccent](./unaccent.md)**: Accent-insensitive text normalization
- **[pg_trgm](./pg_trgm.md)**: Trigram similarity search
