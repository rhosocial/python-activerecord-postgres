# unaccent

`unaccent` 是 PostgreSQL 的 contrib 扩展，用于去除文本中的重音符号（如 `Café` → `Cafe`）。所有支持的 PostgreSQL 版本都带此扩展，但每个数据库仍需单独启用。

💡 *AI 提示词：* "如何将 unaccent 与 pg_trgm 或 tsvector 组合，实现重音不敏感的检索？"

## 安装

```sql
CREATE EXTENSION unaccent;
```

## 用法

工厂函数首参为 dialect。普通字符串视为字面值；要归一化列请传入 `Column` 表达式：

```python
from rhosocial.activerecord.backend.impl.postgres.functions.unaccent import unaccent

sql, params = unaccent(dialect, "Café").to_sql()
# UNACCENT(%s)，参数 ('Café',)

normalized = unaccent(dialect, Column(dialect, "name"))
# UNACCENT("name")
```

可选 `dictionary` 参数指定文本搜索字典：`unaccent(dialect, text, dictionary="unaccent")`。

全文检索时可同时归一化文档与查询（例如在 `to_tsvector(...)` / `to_tsquery(...)` 中）；模糊匹配可用 `pg_trgm` 的 `unaccent(name) % unaccent(query)`。PGroonga 默认使用自己的 `NormalizerAuto`，无需 unaccent。

## 能力探测

```python
dialect.supports_unaccent()              # 扩展已安装
```

## 另见

- **[pg_hint_plan](./pg_hint_plan.md)**：优化器提示
- **[pg_trgm](./pg_trgm.md)**：三元组相似性检索
