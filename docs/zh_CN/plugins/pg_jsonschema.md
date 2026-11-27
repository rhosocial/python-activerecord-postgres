# pg_jsonschema

`pg_jsonschema` 用 [JSON Schema](https://json-schema.org/) 校验 `json` / `jsonb` 值，适合把 ledger / bankcard 的 JSON 载荷约束放到数据库里，而不只在应用层校验。

`postgres-exts` 镜像仅在 **PG14–18**（amd64/arm64）提供，版本 0.3.4，来自上游 GitHub Release。

```python
from rhosocial.activerecord.backend.impl.postgres.functions.pg_jsonschema import (
    jsonb_matches_schema, jsonschema_is_valid, jsonschema_validation_errors,
)
schema = '{"type":"object","properties":{"tags":{"type":"array","items":{"type":"string"}}}}'
sql, params = jsonb_matches_schema(dialect, schema, '{"tags":["vip"]}').to_sql()
```

能力探测：`dialect.supports_pg_jsonschema()`。启用：`CREATE EXTENSION pg_jsonschema;`

工厂会把字面量 schema / instance 转成 `json` / `jsonb`，确保扩展函数签名无歧义。预编译 schema 变体（`*_compiled_schema`）暂未封装。
