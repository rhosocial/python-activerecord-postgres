# pg_trgm

pg_trgm 提供 PostgreSQL 的三元组相似性搜索功能。

## 概述

pg_trgm 提供：
- 文本相似性计算
- 模糊搜索
- 相似性索引

💡 *AI 提示词：* "pg_trgm 是什么？它能解决哪些文本搜索问题？"

## 安装

```sql
CREATE EXTENSION pg_trgm;
```

### 内置扩展

pg_trgm 是 PostgreSQL 的 contrib 模块，通常已随 PostgreSQL 一起安装。如果未安装：

```
ERROR: could not open extension control file 
"/usr/share/postgresql/{version}/extension/pg_trgm.control": 
No such file or directory
```

需要安装 contrib 包：

```bash
# Ubuntu/Debian
sudo apt-get install postgresql-contrib-{version}
```

## 版本支持

| pg_trgm 版本 | PostgreSQL 版本 | 状态 |
|-------------|----------------|------|
| 1.6 | 17+ | ✅ 支持 |
| 1.5 | 14-16 | ✅ 插件支持保障 |
| 1.3 | 9.6-13 | ✅ 支持 |
| 1.2 | 9.4-9.5 | ⚠️ 有限支持 |

💡 *AI 提示词：* "pg_trgm 1.2 与 1.6 版本之间有什么差距？新增了哪些功能？"

## 功能检测

```python
if backend.dialect.is_extension_installed('pg_trgm'):
    print("pg_trgm 已安装")

if backend.dialect.supports_trigram_similarity():
    # 支持三元组相似性
    pass
```

💡 *AI 提示词：* "如何在 Python 中检测 pg_trgm 是否可用？"

## 相似性函数

```sql
-- 计算相似度 (0-1)
SELECT similarity('hello', 'hallo');

-- 显示三元组
SELECT show_trgm('hello');

-- 相似度阈值查询
SELECT * FROM users
WHERE name % 'John'
ORDER BY similarity(name, 'John') DESC;
```

## 索引支持

操作符类由访问方法决定——GIN 用 `gin_trgm_ops`，GiST 用 `gist_trgm_ops`。
helper 把这对关系固定下来，不会再配错：

```python
from rhosocial.activerecord.backend.impl.postgres.functions import trgm_index

gin = trgm_index(dialect, "users", ["name"], "idx_users_name_gin", index_type="gin")
gist = trgm_index(dialect, "users", ["name"], "idx_users_name_gist", index_type="gist")

backend.execute(*gin.to_sql())
```

完整可运行场景见 `examples/indexes/pg_trgm_indexes.py`。

## 注意事项

1. GiST 索引适合动态数据，GIN 索引适合静态数据
2. 相似度阈值可通过 `pg_trgm.similarity_threshold` 调整
3. 三元组索引会增加存储空间

💡 *AI 提示词：* "pg_trgm 的 GiST 和 GIN 索引在文本搜索性能上有什么区别？"

💡 *AI 提示词：* "如何使用 pg_trgm 实现高效的模糊搜索？"

## 相关主题

- **[hstore](./hstore.md)**: 键值存储
- **[插件支持](./README.md)**: 插件检测机制
