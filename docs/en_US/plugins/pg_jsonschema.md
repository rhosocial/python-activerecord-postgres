# pg_jsonschema

`pg_jsonschema` validates `json` / `jsonb` values against [JSON Schema](https://json-schema.org/). It is useful for enforcing the shape of ledger or bankcard payloads inside the database rather than only in application code.

Available in `postgres-exts` images for **PG14–18** only (amd64/arm64), version 0.3.4 from the upstream GitHub release.

```python
from rhosocial.activerecord.backend.impl.postgres.functions.pg_jsonschema import (
    jsonb_matches_schema, jsonschema_is_valid, jsonschema_validation_errors,
)
schema = '{"type":"object","properties":{"tags":{"type":"array","items":{"type":"string"}}}}'
sql, params = jsonb_matches_schema(dialect, schema, '{"tags":["vip"]}').to_sql()
```

Capability: `dialect.supports_pg_jsonschema()`. Creation: `CREATE EXTENSION pg_jsonschema;`

Literal schemas and instances are cast to `json` / `jsonb` by the factories, so the extension's exact signatures resolve without ambiguity. Compiled-schema variants (`*_compiled_schema`) are not wrapped yet.
