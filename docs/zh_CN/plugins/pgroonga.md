# PGroonga

PGroonga 是基于 [Groonga](https://groonga.org/) 的 PostgreSQL 高速全文检索扩展。与 PostgreSQL 内置的 `tsvector` 检索不同，默认的 `TokenBigram` 分词器直接支持中文、日文、韩文，无需配置文本搜索配置或专用分词器。

💡 *AI 提示词：* "多语言全文检索场景下，什么时候应该优先选择 PGroonga 而不是 tsvector 或 pg_trgm？"

## 安装

PGroonga 不属于 PostgreSQL 或 contrib，必须先在**服务器端**安装。Debian/Ubuntu 上：

```bash
wget https://packages.groonga.org/debian/groonga-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y ./groonga-apt-source-latest-*.deb
sudo apt update
sudo apt install -y postgresql-<version>-pgdg-pgroonga
```

然后在每个数据库中启用：

```sql
CREATE EXTENSION pgroonga;
```

本项目使用的 `postgres-exts` 镜像在 PG12（3.2.4）与 PG14–18（4.1.0）上预装了 PGroonga，版本矩阵见[镜像仓库](https://github.com/vistart/Dockerfiles)。

## 能力探测

```python
if backend.dialect.is_extension_installed("pgroonga"):
    print("PGroonga 版本:", backend.dialect.get_extension_version("pgroonga"))
    print("关键词检索:", backend.dialect.supports_pgroonga_match())
```

能力判断按版本区分：RLS 需 2.3.3+，`&=~` 需 3.0.0+，语义检索需 4.0.5+ 且安装 `groonga-plugin-language-model`。

## 索引 DDL

通过 dialect helper 构造类型化的 `PostgresCreateIndexExpression`（访问方法 `pgroonga`，默认 opclass 为 `pgroonga_text_full_text_search_ops_v2`）：

```python
expr = backend.dialect.build_pgroonga_index_expression(
    "idx_docs_body", "docs", ["body"], tokenizer="TokenMecab"
)
sql, params = expr.to_sql()
backend.execute(sql, params)
# CREATE INDEX "idx_docs_body" ON "docs" USING pgroonga
#   ("body" pgroonga_text_full_text_search_ops_v2) WITH (tokenizer = 'TokenMecab')
```

Groonga 选项（`tokenizer`、`token_filters`、`plugins`、`normalizers`）在渲染为 SQL 字符串字面量前会经过允许列表校验。`jsonb` 列可传 `opclasses={"payload": "pgroonga_jsonb_ops_v2"}`。

## 检索

所有工厂函数位于 `functions/pgroonga.py`：首参为 dialect，查询文本走绑定参数，普通字符串列名视为列引用：

```python
from rhosocial.activerecord.backend.impl.postgres.functions.pgroonga import (
    pgroonga_match, pgroonga_query, pgroonga_score,
)

# 关键词检索（v2 运算符 &@）；默认分词器即可检索中文
where = pgroonga_match(dialect, "body", "全文检索")

# 查询语言：AND / OR / NOT、短语（&@~）
where = pgroonga_query(dialect, "body", "groonga OR postgresql")

# 排序打分：pgroonga_score 仅在走索引扫描时非零
query = QueryStatement(
    dialect=dialect,
    select=[Column(dialect, "id"), pgroonga_score(dialect).as_("score")],
    from_=Table(dialect, "docs"),
    where=pgroonga_match(dialect, "body", "Groonga"),
)
```

小表可能被规划器选为顺序扫描，此时 `pgroonga_score()` 返回 0；测试中如需断言分数请用 `SET enable_seqscan = off` 强制索引扫描。

片段/高亮：`pgroonga_snippet_html`、`pgroonga_highlight_html`、`pgroonga_match_positions_byte` / `_character`；查询辅助：`pgroonga_query_escape`、`pgroonga_query_extract_keywords`。

前缀（`&^`、`&^|`）和正则（`&~`、`&~|`）检索需分别使用 `pgroonga_text_term_search_ops_v2` / `pgroonga_text_regexp_ops_v2` opclass。`@@`、`%%`、`&?` 是已弃用的 v1 写法，本适配不会生成。

## 另见

- **[pg_trgm](./pg_trgm.md)**：短字符串与容错拼写相似度的三元组检索
- **[全文检索](../postgres_specific_features/full_text_search.md)**：PostgreSQL 原生 tsvector 检索
- **[pgvector](./pgvector.md)**：向量相似度检索
