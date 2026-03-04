# hstore

hstore 提供 PostgreSQL 的键值对存储功能。

## 概述

hstore 提供：
- **hstore** 数据类型
- 键值操作符
- 索引支持

💡 *AI 提示词：* "hstore 是什么？在什么场景下应该使用 hstore？"

## 安装

```sql
CREATE EXTENSION hstore;
```

### 内置扩展

hstore 是 PostgreSQL 的 contrib 模块，通常已随 PostgreSQL 一起安装。如果未安装：

```
ERROR: could not open extension control file 
"/usr/share/postgresql/{version}/extension/hstore.control": 
No such file or directory
```

需要安装 contrib 包：

```bash
# Ubuntu/Debian
sudo apt-get install postgresql-contrib-{version}
```

## 版本支持

| hstore 版本 | PostgreSQL 版本 | 状态 |
|------------|----------------|------|
| 1.8 | 17+ | ✅ 支持 |
| 1.6 | 14-16 | ✅ 插件支持保障 |
| 1.4 | 9.6-13 | ✅ 支持 |
| 1.3 | 9.4-9.5 | ⚠️ 有限支持 |

💡 *AI 提示词：* "hstore 1.3 与 1.8 版本之间有什么差距？"

## 功能检测

```python
if backend.dialect.is_extension_installed('hstore'):
    print("hstore 已安装")

if backend.dialect.supports_hstore_type():
    # 支持 hstore 类型
    pass
```

💡 *AI 提示词：* "如何在 Python 中检测 hstore 是否可用？"

## 数据类型

```sql
-- 创建带 hstore 列的表
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    attributes hstore
);

-- 插入数据
INSERT INTO users (name, attributes)
VALUES ('张三', '"age"=>"30", "city"=>"北京", "role"=>"admin"');
```

## 常用操作

```sql
-- 获取值
SELECT attributes -> 'age' FROM users;

-- 包含键
SELECT * FROM users WHERE attributes ? 'city';

-- 包含键值对
SELECT * FROM users WHERE attributes @> '"city"=>"北京"';

-- 获取所有键
SELECT akeys(attributes) FROM users;
```

## 索引支持

```sql
-- GIN 索引（支持 @>, ?, ?& 操作符）
CREATE INDEX idx_users_attrs ON users USING GIN(attributes);

-- GiST 索引
CREATE INDEX idx_users_attrs ON users USING GIST(attributes);
```

## hstore vs JSONB

| 特性 | hstore | JSONB |
|-----|--------|-------|
| 值类型 | 仅字符串 | 任意 JSON 类型 |
| 嵌套 | 不支持 | 支持 |
| 索引 | GIN/GiST | GIN |
| 性能 | 更快（简单结构） | 灵活性更高 |

💡 *AI 提示词：* "PostgreSQL 的 hstore 和 JSONB 在键值存储场景下各有什么优劣？"

💡 *AI 提示词：* "什么时候应该选择 hstore 而不是 JSONB？"

## 相关主题

- **[pg_trgm](./pg_trgm.md)**: 三元组相似性
- **[插件支持](./README.md)**: 插件检测机制
