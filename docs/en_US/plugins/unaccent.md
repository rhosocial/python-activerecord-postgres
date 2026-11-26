# unaccent

`unaccent` is a PostgreSQL contrib extension that removes accents from text (e.g. `Café` → `Cafe`). It is available on every supported PostgreSQL version, but must be enabled per database.

💡 *AI Prompt:* "How should unaccent be combined with pg_trgm or tsvector for accent-insensitive search?"

## Installation

```sql
CREATE EXTENSION unaccent;
```

## Usage

Factories take the dialect first. A plain string is a literal value; pass a `Column` expression to normalize a column:

```python
from rhosocial.activerecord.backend.impl.postgres.functions.unaccent import unaccent

sql, params = unaccent(dialect, "Café").to_sql()
# UNACCENT(%s) with params ('Café',)

normalized = unaccent(dialect, Column(dialect, "name"))
# UNACCENT("name")
```

An optional dictionary selects a specific text search dictionary: `unaccent(dialect, text, dictionary="unaccent")`.

For full text search, normalize both the document and the query, for example inside `to_tsvector(...)` / `to_tsquery(...)`. For fuzzy matching, `unaccent(name) % unaccent(query)` works with `pg_trgm`. PGroonga uses its own normalizer (`NormalizerAuto` by default) instead.

## Capability detection

```python
dialect.supports_unaccent()              # extension installed
```

## See also

- **[pg_hint_plan](./pg_hint_plan.md)**: Optimizer hints
- **[pg_trgm](./pg_trgm.md)**: Trigram similarity search
