# PostgreSQL Dialect 表达式

## DDL 语句

### CREATE TABLE ... LIKE

PostgreSQL 支持使用 `LIKE` 子句复制表结构，并通过 INCLUDING/EXCLUDING 选项精细控制要复制的内容。

```python
from rhosocial.activerecord.backend.expression import CreateTableExpression
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect

# 基本用法 - 复制表结构
create_expr = CreateTableExpression(
    dialect=PostgresDialect(),
    table_name="users_copy",
    columns=[],
    dialect_options={'like_table': 'users'}
)
# 生成: CREATE TABLE "users_copy" (LIKE "users")

# 带 INCLUDING 选项（字典格式 - 推荐）
create_expr = CreateTableExpression(
    dialect=PostgresDialect(),
    table_name="users_copy",
    columns=[],
    dialect_options={
        'like_table': 'users',
        'like_options': {
            'including': ['DEFAULTS', 'CONSTRAINTS', 'INDEXES'],
            'excluding': ['COMMENTS']
        }
    }
)
# 生成: CREATE TABLE "users_copy" (LIKE "users", INCLUDING DEFAULTS,
#        INCLUDING CONSTRAINTS, INCLUDING INDEXES, EXCLUDING COMMENTS)

# 带模式限定的源表
create_expr = CreateTableExpression(
    dialect=PostgresDialect(),
    table_name="users_copy",
    columns=[],
    dialect_options={'like_table': ('public', 'users')}
)
# 生成: CREATE TABLE "users_copy" (LIKE "public"."users")

# 带临时表和 IF NOT EXISTS
create_expr = CreateTableExpression(
    dialect=PostgresDialect(),
    table_name="temp_users",
    columns=[],
    temporary=True,
    if_not_exists=True,
    dialect_options={'like_table': 'users'}
)
# 生成: CREATE TEMPORARY TABLE IF NOT EXISTS "temp_users" (LIKE "users")

# 包含所有内容
create_expr = CreateTableExpression(
    dialect=PostgresDialect(),
    table_name="users_copy",
    columns=[],
    dialect_options={
        'like_table': 'users',
        'like_options': {'including': ['ALL']}
    }
)
# 生成: CREATE TABLE "users_copy" (LIKE "users", INCLUDING ALL)
```

**可用的 INCLUDING/EXCLUDING 选项：**
- `DEFAULTS` - 复制默认值
- `CONSTRAINTS` - 复制表约束
- `INDEXES` - 复制索引
- `IDENTITY` - 复制标识列
- `GENERATED` - 复制生成列
- `COMMENTS` - 复制列/表注释
- `STORAGE` - 复制存储参数
- `COMPRESSION` - 复制压缩设置
- `ALL` - 复制所有内容

**重要说明：**
- 当 `dialect_options` 中指定 `like_table` 时，具有最高优先级
- 所有其他参数（columns、indexes、constraints 等）都会被忽略
- 只有 `temporary` 和 `if_not_exists` 标志会被考虑
- `like_options` 键支持字典和列表两种格式

## RETURNING 子句

PostgreSQL 支持 DML 操作的 RETURNING：

```python
# INSERT RETURNING
user = User(name="张三")
user.save()
# 返回带有生成 id 的插入行

# UPDATE RETURNING
User.query().where(User.c.id == 1).update(name="李四")
# 返回受影响的行
```

## DISTINCT ON

PostgreSQL 特有的 DISTINCT ON：

```python
# 获取每个用户的最新订单
orders = Order.query().distinct_on("user_id").order_by(
    "user_id", "created_at DESC"
).all()
```

## ILIKE（不区分大小写匹配）

```python
users = User.query().where(
    "name ILIKE ?", ("%张%",)
).all()
```

## 数组操作符

```python
# 包含 (@>)
Article.query().where("tags @> ?", (['python', 'database'],))

# 被包含 (<@)
Article.query().where("tags <@ ?", (['python', 'database', 'web'],))

# 重叠 (&&)
Article.query().where("tags && ?", (['python', 'java'],))

# 任意元素
Article.query().where("? = ANY(tags)", ('python',))
```

## JSONB 操作符

```python
# 获取路径上的 JSON 值
Product.query().where("attributes->>'brand' = ?", ('Dell',))

# 获取嵌套值
Product.query().where("attributes->'specs'->>'cpu' = ?", ('Intel i7',))

# JSONB 包含
Product.query().where("attributes @> ?", ({"brand": "Dell"},))

# 键存在
Product.query().where("attributes ? 'brand'", ())
```

💡 *AI 提示词：* "PostgreSQL 的 ILIKE 和标准 LIKE 在性能上有什么区别？"
