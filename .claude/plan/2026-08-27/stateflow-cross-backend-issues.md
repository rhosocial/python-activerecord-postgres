# PostgreSQL 后端在 stateflow 跨后端测试中暴露的问题

> 发现日期：2026-08-27
> 发现途径：python-stateflow 跨后端测试（stateflow 作为 rhosocial-activerecord 的 realworld 测试案例）
> 测试配置：PostgreSQL 19 beta3，192.168.1.3:16690，database=test_db

## 问题概述

stateflow 的 9 个模型表全部使用 `TextType`（存 UUID/datetime/JSON）和 `BooleanType`（存 bool），
通过 `CreateTableExpression` 按 PostgreSQL dialect 编译 DDL 后建表成功，但 INSERT/SELECT 时
出现以下系统性错误。

## 问题 1：JSON / list 字段读写类型不匹配

### 现象

```
TypeError: Cannot convert str to list
```

出现 52 次，影响所有 `list` / `dict` 类型字段（如 `terminal_states`、`advance_states`、
`context`、`payload`、`depends_on` 等）。

### 原因分析

stateflow 模型字段声明为 `dict` / `list`（裸类型），SQLite backend 通过
`SQLiteJSONAdapter` 自动做 `json.dumps` / `json.loads` 转换。

PostgreSQL backend 的 JSON adapter（`JsonType` / `JsonBType`）可能在以下环节缺失：

1. **INSERT 时**：Python `dict` / `list` → PostgreSQL TEXT 列。
   SQLite 的 `SQLiteJSONAdapter` 会先 `json.dumps` 再写入；
   PostgreSQL 的 adapter 可能没有注册 `dict → str` / `list → str` 的转换，
   导致直接传入 Python 对象，psycopg 不识别。

2. **SELECT 时**：PostgreSQL TEXT 列 → Python `dict` / `list`。
   SQLite 的 `SQLiteJSONAdapter` 会 `json.loads` 反序列化；
   PostgreSQL 的 adapter 可能没有做 `str → dict` / `str → list` 的反向转换，
   返回原始字符串，Pydantic 校验时报 `Cannot convert str to list`。

### 复现方法

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

config = PostgresConnectionConfig(host="192.168.1.3", port=16690,
                                  database="test_db", username="root", password="password")
# 配置模型，建表（TextType 列），INSERT 含 dict/list 字段，再 SELECT 回来
# 会发现返回的是 str 而非 dict/list
```

### 建议修复

1. 检查 PostgreSQL backend 的 `get_default_adapter_suggestions()` 是否注册了
   `dict → str` 和 `list → str` 的 JSON adapter（类似 SQLite 的 `SQLiteJSONAdapter`）。
2. 如果 PostgreSQL dialect 把 `TextType` 映射为 `TEXT`，则需要在 adapter 层做
   JSON 序列化/反序列化。
3. 考虑使用 PostgreSQL 原生 `JSONB` 类型替代 `TEXT` 存储 JSON 数据——但
   `CreateTableExpression` 中的 `TextType` 是否应按 dialect 映射为 `JSONB`？
   这可能需要在 dialect 层做类型映射。

---

## 问题 2：Boolean 字段类型不匹配

### 现象

```
psycopg.errors.DatatypeMismatch: column "skipped" is of type integer but expression is of type boolean
```

出现 55 次。

### 原因分析

stateflow schema 中 `skipped`、`is_reversible`、`conflict` 使用 `BooleanType`，
DDL 编译后在 PostgreSQL 中创建为 `BOOLEAN` 列。

但 stateflow 的 Pydantic 模型声明为 `bool`，`BooleanAdapter` 在 SQLite 中用 `0/1`，
而 PostgreSQL 的 `BOOLEAN` 列期望 `True/False` 而非 `0/1`。

可能的原因：
- `BooleanAdapter` 的 `to_database` 统一返回 `0/1`（SQLite 兼容），
  但 PostgreSQL 需要原生 `True/False`。
- 或者 `get_column_adapters()` 对 `bool` 类型的注册在 PostgreSQL backend 中
  没有正确映射。

### 建议修复

1. 检查 PostgreSQL backend 的 `BooleanAdapter` 是否将 Python `bool` 转为
   PostgreSQL 兼容的值（`True/False` 或 `t/f`）。
2. 或者让 `BooleanType` 在 PostgreSQL dialect 中映射为 `INTEGER` 而非 `BOOLEAN`，
   与 SQLite 保持一致。
3. 确认 `get_default_adapter_suggestions()` 中 `bool` 类型的注册。

---

## 问题 3：RETURNING 子句占位符问题

### 现象

```
HINT: You will need to rewrite or cast the expression.
```

出现 110 次，伴随 `$4, $5, ...` 占位符。

### 原因分析

PostgreSQL backend 的 INSERT 使用 `RETURNING` 子句返回自增列。
但 `OptimisticLockMixin` 的 `version` 列 UPDATE 表达式使用了
`"version" + 1` 形式，在 PostgreSQL 中可能需要显式 CAST：

```sql
-- SQLite (works)
UPDATE ... SET "version" = "version" + 1 WHERE "id" = ? AND "version" = ?

-- PostgreSQL (may need CAST)
UPDATE ... SET "version" = "version" + 1 WHERE "id" = $1 AND "version" = $2
```

也可能是 `$N` 占位符与 `?` 占位符的混用——psycopg 使用 `%s` 或 `$N`，
而 ActiveRecord 的 SQL 构建可能对 PostgreSQL dialect 的占位符风格处理不完整。

### 建议修复

1. 检查 PostgreSQL dialect 的占位符风格（`%s` vs `$N` vs `?`）是否一致。
2. 检查 `RETURNING` 子句中列名是否需要引号。
3. 检查 `OptimisticLockMixin.get_update_expression()` 在 PostgreSQL 中的
   SQL 生成是否正确。

---

## 测试环境

- PostgreSQL 19 beta3 (192.168.1.3:16690)
- python-activerecord-postgres 1.0.0.dev17
- psycopg 3.3.4 (binary)
- Python 3.14.4
- stateflow 1.0.0.dev1 (commit 5a9a5dc)

## stateflow 测试命令

```bash
cd python-stateflow
# 只跑 PostgreSQL 测试
pytest -k "postgres"
```

## 当前测试结果

| 后端 | sync | async | 状态 |
|------|------|-------|------|
| SQLite | ✅ 全过 | ✅ 全过 | 正常 |
| MySQL | ⚠️ 部分失败 | ⚠️ 部分失败 | TEXT DEFAULT 已修，RETURNING 待查 |
| MariaDB | ⚠️ 部分失败 | ⚠️ 部分失败 | 同 MySQL |
| PostgreSQL | ❌ 大量失败 | ❌ 大量失败 | 上述 3 个问题 |
