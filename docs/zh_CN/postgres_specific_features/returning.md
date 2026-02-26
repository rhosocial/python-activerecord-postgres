# RETURNING 子句

PostgreSQL 的 RETURNING 子句返回被修改行的数据。

## INSERT RETURNING

```python
user = User(name="张三", email="zhangsan@example.com")
user.save()

# user 对象现在包含所有生成的值
print(user.id)        # 自动生成
print(user.created_at)  # 默认值
```

## UPDATE RETURNING

```python
# 带返回的更新
result = User.query().where(
    User.c.id == 1
).update(name="李四")

# 返回更新的行
```

## DELETE RETURNING

```python
# 带返回的删除
result = User.query().where(
    User.c.id == 1
).delete()

# 返回被删除的行
```

## 使用场景

1. **获取自动生成的 ID**：INSERT 后获取 serial/identity 值
2. **审计变更**：记录实际被修改的值
3. **级联信息**：删除前获取相关数据

💡 *AI 提示词：* "RETURNING 如何相比单独的 SELECT 查询提高效率？"
