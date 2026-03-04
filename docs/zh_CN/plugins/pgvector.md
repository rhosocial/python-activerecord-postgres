# pgvector

pgvector 是 PostgreSQL 的向量相似性搜索扩展，专为 AI 和机器学习应用设计。

## 概述

pgvector 提供：
- **vector** 数据类型
- 向量相似性搜索（`<->` 操作符）
- IVFFlat 和 HNSW 索引

💡 *AI 提示词：* "pgvector 是什么？它在 AI 应用中有什么作用？"

## 安装

```sql
CREATE EXTENSION vector;
```

### 未安装时的错误

如果服务器未安装 pgvector，执行 `CREATE EXTENSION vector` 会报错：

```
ERROR: could not open extension control file 
"/usr/share/postgresql/{version}/extension/vector.control": 
No such file or directory
```

这表示 pgvector 未在服务器上安装。需要先安装 pgvector：

```bash
# Ubuntu/Debian
sudo apt-get install postgresql-{version}-pgvector

# 或从源码编译
git clone https://github.com/pgvector/pgvector.git
cd pgvector
make
sudo make install
```

### 权限要求

安装扩展需要超级用户权限。

## 版本支持

| pgvector 版本 | 功能 | 发布日期 |
|--------------|------|---------|
| 0.8.x | 最新版，支持更多距离函数 | 2024+ |
| 0.7.x | 改进的 HNSW 索引 | 2024 |
| 0.6.x | 并行索引构建 | 2023 |
| 0.5.x | HNSW 索引 | 2023 |
| 0.4.x | IVFFlat 索引改进 | 2023 |
| 0.1.x | 基础向量类型 | 2022 |

**推荐版本**: 0.5.0+（支持 HNSW 索引）

💡 *AI 提示词：* "pgvector 0.1 与最新版本 0.8 之间有什么主要差距？升级需要注意什么？"

## 功能检测

```python
# 检查 pgvector 是否已安装
if backend.dialect.is_extension_installed('vector'):
    print("pgvector 已安装")

# 获取版本
version = backend.dialect.get_extension_version('vector')
print(f"pgvector 版本: {version}")

# 检查功能支持
if backend.dialect.supports_vector_type():
    # 支持 vector 类型
    pass

if backend.dialect.supports_hnsw_index():
    # 支持 HNSW 索引（需要 0.5.0+）
    pass
```

## 数据类型

```sql
-- 创建带向量列的表
CREATE TABLE embeddings (
    id SERIAL PRIMARY KEY,
    content TEXT,
    embedding vector(1536)  -- OpenAI embedding 维度
);
```

## 向量索引

### HNSW 索引（推荐）

```sql
-- 创建 HNSW 索引（需要 pgvector 0.5.0+）
CREATE INDEX idx_embeddings_hnsw ON embeddings 
USING hnsw (embedding vector_cosine_ops);
```

### IVFFlat 索引

```sql
-- 创建 IVFFlat 索引
CREATE INDEX idx_embeddings_ivfflat ON embeddings 
USING ivfflat (embedding vector_cosine_ops) 
WITH (lists = 100);
```

## 相似性搜索

```sql
-- 余弦相似性搜索（找出最相似的5条记录）
SELECT content, embedding <=> '[0.1, 0.2, ...]'::vector AS distance
FROM embeddings
ORDER BY embedding <=> '[0.1, 0.2, ...]'::vector
LIMIT 5;
```

## 注意事项

1. 向量最大维度：2000
2. HNSW 索引需要 pgvector 0.5.0+
3. 索引创建可能耗时较长
4. 建议在数据加载后创建索引

💡 *AI 提示词：* "pgvector 的 HNSW 和 IVFFlat 索引有什么区别？各自的适用场景是什么？"

💡 *AI 提示词：* "如何选择 pgvector 索引的参数以获得最佳性能？"

## 相关主题

- **[PostGIS](./postgis.md)**: 空间数据库功能
- **[插件支持](./README.md)**: 插件检测机制
