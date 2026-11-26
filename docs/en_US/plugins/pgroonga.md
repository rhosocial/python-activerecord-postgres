# PGroonga

PGroonga is a high-speed full text search extension for PostgreSQL backed by [Groonga](https://groonga.org/). Unlike PostgreSQL's built-in `tsvector` search, it handles Chinese, Japanese and Korean text with the default `TokenBigram` tokenizer — no text search configuration or per-language parser is needed.

💡 *AI Prompt:* "When should I prefer PGroonga over tsvector or pg_trgm for multilingual search?"

## Installation

PGroonga is not part of PostgreSQL or contrib; it must be installed on the **server** first. On Debian/Ubuntu:

```bash
wget https://packages.groonga.org/debian/groonga-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y ./groonga-apt-source-latest-*.deb
sudo apt update
sudo apt install -y postgresql-<version>-pgdg-pgroonga
```

Then enable it in each database:

```sql
CREATE EXTENSION pgroonga;
```

The `postgres-exts` images used by this project ship PGroonga on PG12 (3.2.4) and PG14–18 (4.1.0). See the [image repository](https://github.com/vistart/Dockerfiles) for the version matrix.

## Detection

```python
if backend.dialect.is_extension_installed("pgroonga"):
    print("PGroonga version:", backend.dialect.get_extension_version("pgroonga"))
    print("Keyword search:", backend.dialect.supports_pgroonga_match())
```

Capabilities are version-aware: `rls` requires 2.3.3+, `equal_query` (`&=~`) 3.0.0+, and semantic search 4.0.5+ plus the `groonga-plugin-language-model` package.

## Index DDL

Use the dialect helper; it builds a typed `PostgresCreateIndexExpression` (access method `pgroonga`, default opclass `pgroonga_text_full_text_search_ops_v2`):

```python
expr = backend.dialect.build_pgroonga_index_expression(
    "idx_docs_body", "docs", ["body"], tokenizer="TokenMecab"
)
sql, params = expr.to_sql()
backend.execute(sql, params)
# CREATE INDEX "idx_docs_body" ON "docs" USING pgroonga
#   ("body" pgroonga_text_full_text_search_ops_v2) WITH (tokenizer = 'TokenMecab')
```

Groonga option values (`tokenizer`, `token_filters`, `plugins`, `normalizers`) are validated against an allow-list before they are rendered as SQL string literals. For a `jsonb` column pass `opclasses={"payload": "pgroonga_jsonb_ops_v2"}`.

## Searching

All factories live in `functions/pgroonga.py`, take the dialect first, bind query text as parameters, and treat a plain string column as a column reference:

```python
from rhosocial.activerecord.backend.impl.postgres.functions.pgroonga import (
    pgroonga_match, pgroonga_query, pgroonga_match_in,
    pgroonga_score, pgroonga_highlight_html,
)

# Keyword search (v2 operator &@); works for Chinese with the default tokenizer
where = pgroonga_match(dialect, "body", "全文检索")

# Query language: AND / OR / NOT and quoted phrases (&@~)
where = pgroonga_query(dialect, "body", "groonga OR postgresql")

# Ranking: pgroonga_score is non-zero only on an index scan
query = QueryStatement(
    dialect=dialect,
    select=[Column(dialect, "id"), pgroonga_score(dialect).as_("score")],
    from_=Table(dialect, "docs"),
    where=pgroonga_match(dialect, "body", "Groonga"),
)
```

On very small tables the planner may choose a sequential scan, which makes `pgroonga_score()` return 0. Force an index scan (`SET enable_seqscan = off`) when asserting scores in tests.

Snippet / highlight helpers: `pgroonga_snippet_html`, `pgroonga_highlight_html`, `pgroonga_match_positions_byte`, `pgroonga_match_positions_character`. Query helpers: `pgroonga_query_escape`, `pgroonga_query_extract_keywords`.

Prefix (`&^`, `&^|`) and regexp (`&~`, `&~|`) searches need their respective operator classes (`pgroonga_text_term_search_ops_v2` / `pgroonga_text_regexp_ops_v2`). The `@@`, `%%` and `&?` spellings are v1 and deliberately not emitted.

## See also

- **[pg_trgm](./pg_trgm.md)**: Trigram similarity for short strings and typo tolerance
- **[Full text search](../postgres_specific_features/full_text_search.md)**: PostgreSQL's native tsvector search
- **[pgvector](./pgvector.md)**: Vector similarity search
