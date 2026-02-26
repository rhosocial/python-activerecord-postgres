# PostgreSQL Dialect 表达式

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
