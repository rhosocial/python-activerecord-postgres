# 测试

本节介绍 PostgreSQL 后端的测试。

## 目录

- [测试配置](configuration.md): 测试环境设置
- [本地 PostgreSQL 测试](local.md): 本地数据库测试

## 测试原则

### 同步/异步对称性

所有具有 IO 操作的后端必须为等效场景准备**成对的同步和异步测试**：

```python
# 同步测试
def test_create_user():
    user = User(name="Alice").create()
    assert user.id is not None

# 异步测试 — 相同逻辑，异步 API
async def test_async_create_user():
    user = await AsyncUser(name="Alice").create()
    assert user.id is not None
```

如果后端仅支持同步或仅支持异步，则只准备相应的测试。

### 表达式类 — 无 IO

表达式测试不涉及数据库 IO — 它们只构建 SQL 并验证生成的 SQL：

```python
def test_expression_sql():
    expr = Eq(User.name, "Alice")
    assert expr.to_sql(dialect) == "`name` = %s"
    assert expr.params == ["Alice"]
```

表达式测试不需要异步对应测试。

### ActiveRecord 测试 — 使用测试套件

ActiveRecord 功能测试（模型 CRUD、关系、查询）使用**测试套件**：

```
python-activerecord-testsuite/
└── src/rhosocial/activerecord/testsuite/feature/
    ├── basic/      # 第 1 级 — 必须首先通过
    ├── relation/   # 第 1 级 — 必须首先通过
    ├── query/      # 第 1 级 — 必须首先通过
    ├── events/     # 第 2 级 — 扩展行为
    ├── mixins/     # 第 2 级
    ├── interface/  # 第 2 级
    └── examples/   # 第 2 级
```

每个后端提供**提供者实现**，将测试连接到其特定数据库。测试逻辑是共享的；只有提供者层因后端而异。

**运行测试套件测试：**

```bash
cd python-activerecord-postgres
PYTHONPATH=tests .venv3.14-ubuntu26.04/bin/pytest \
    ../python-activerecord-testsuite/src/rhosocial/activerecord/testsuite/feature/relation/
```

**提供者职责：** 参见[核心测试套件提供者指南](provider_guide.md)。

### 测试类别摘要

| 测试内容 | 方法 | IO？ | 异步？ |
|----------|------|------|--------|
| 表达式类（dialect SQL 生成） | 单元测试，无数据库 | 否 | 否 |
| 类型适配器（类型转换） | 单元测试，无数据库 | 否 | 否 |
| 命名功能（连接、表达式、过程、迁移） | 后端 CLI 脚本 | 是 | 如果支持 |
| ActiveRecord 功能（CRUD、关系、查询） | 测试套件 + 提供者 | 是 | 是 |
| 后端特定功能（唯一类型、语法） | 项目特定测试 | 是 | 是 |

## 提供者职责

作为后端实现，PostgreSQL 后端必须实现 Provider 接口来处理测试环境的设置和清理。这对于测试隔离和正确性至关重要。

### 关键原则

1. **环境准备**: 提供者必须：
   - 创建数据库 schema（表、索引、类型）
   - 建立数据库连接
   - 使用 PostgreSQL 特定实现配置测试模型

2. **环境清理**: 提供者必须：
   - 在每个测试后删除所有测试表
   - 删除自定义类型（如果有）
   - 正确关闭所有游标
   - 断开数据库连接

### 关键：清理顺序

清理必须按以下顺序进行以避免问题：

```
正确顺序：
1. DROP TABLE 语句（清理数据）
2. 删除自定义类型
3. 关闭游标
4. 断开连接

错误顺序：
1. 先断开连接  ❌
2. 然后清理  ❌ (连接已关闭！)
```

### 常见问题

- **表冲突**：不删除表可能导致"表已存在"错误
- **类型冲突**：PostgreSQL 自定义类型（如 ENUM、ARRAY）需要删除
- **数据污染**：不清理可能导致上下文相关测试失败
- **连接问题**：不当清理可能导致资源耗尽

### 实现参考

详细实现指南请参阅测试套件文档：
- `python-activerecord-testsuite/docs/zh_CN/README.md`
