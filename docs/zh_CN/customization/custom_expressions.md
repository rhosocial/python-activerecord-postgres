# 自定义表达式

## 概述

rhosocial-activerecord 的表达式系统是可扩展的。您可以为表达式库未覆盖的数据库特定 SQL 语法创建自定义表达式类。

## 表达式设计原则

在创建自定义表达式之前，请理解以下核心原则：

### 1. 表达式是声明式的，不是命令式的

表达式实例**收集影响 SQL 生成的所有参数**。它是一个数据容器，而不是 SQL 生成器。表达式本身不生成 SQL — 它委托给 dialect 的格式化函数。

```python
# 此表达式收集参数，而不是生成 SQL
expr = ComparisonPredicate(dialect, column, operator='>=', value=literal)
# expr.dialect, expr.column, expr.operator, expr.value 都被存储
```

### 2. 委托给 Dialect

调用 `to_sql()` 时，表达式委托给其绑定的 dialect 的 `format_*()` 方法。Dialect 根据以下因素决定实际 SQL 语法：
- 后端类型（MySQL、PostgreSQL、SQLite 等）
- Dialect 版本（如 PostgreSQL 14 vs 16）
- 功能标志（如 `supports_returning_insert()`）
- 表达式的参数

```python
class MyExpression(SQLValueExpression):
    def to_sql(self) -> SQLQueryAndParams:
        # 委托给 dialect — dialect 决定 SQL 语法
        return self.dialect.format_my_expression(self._inner)
```

### 3. 数据类型是表达式

DataType 类像其他表达式一样继承自 `BaseExpression`。它们遵循相同的委托模式：`to_sql()` 调用 `dialect.format_data_type(self)`。这意味着 DataType 与其他表达式享有相同的可扩展性。

### 4. 序列化支持

因为表达式实例收集影响 SQL 生成的所有参数，所以它们可以被**序列化**（如 JSON）和**反序列化**回来。这支持：
- 缓存查询计划
- 跨进程传输查询
- 存储查询定义以供稍后执行

序列化往返保留了重新生成 SQL 所需的所有信息。

### 5. 组合优于继承

表达式通过组合 mixin 获得功能，而不是通过深层继承层次结构。`DistanceExpression` 可以通过混入 `ComparisonMixin` 获得比较运算符，而无需继承自"可比较表达式"基类。

## 表达式基类

所有表达式继承自 `rhosocial.activerecord.backend.expression.bases` 中的以下基类之一：

| 基类 | 用途 | 使用场景 |
|------|------|---------|
| `SQLValueExpression` | 返回非布尔值（整数、字符串、日期等） | 创建函数调用或计算值 |
| `SQLPredicate` | 返回布尔值 | 创建条件或过滤器 |

两者都需要实现 `to_sql()`，返回 `(sql_string, params_tuple)`。

## 创建自定义值表达式

继承 `SQLValueExpression` 用于产生值的表达式：

```python
from rhosocial.activerecord.backend.expression.bases import SQLValueExpression, SQLQueryAndParams


class RegexMatchExpression(SQLValueExpression):
    """自定义表达式：column REGEXP pattern。"""

    def __init__(self, dialect, column, pattern):
        super().__init__(dialect)
        self._column = column
        self._pattern = pattern

    def to_sql(self) -> SQLQueryAndParams:
        col_sql, col_params = self._column.to_sql()
        return f"{col_sql} ~ ?", col_params + (self._pattern,)
```

用法：

```python
# 假设 User.c.name 是一个 Column 表达式
expr = RegexMatchExpression(dialect, User.c.name, r'^admin.*')
query = User.query().where(expr)
```

## 创建自定义谓词

继承 `SQLPredicate` 用于产生布尔值的表达式：

```python
from rhosocial.activerecord.backend.expression.bases import SQLPredicate, SQLQueryAndParams


class FuzzyMatchPredicate(SQLPredicate):
    """自定义谓词：column FUZZY_MATCH pattern。"""

    def __init__(self, dialect, column, pattern, threshold=0.8):
        super().__init__(dialect)
        self._column = column
        self._pattern = pattern
        self._threshold = threshold

    def to_sql(self) -> SQLQueryAndParams:
        col_sql, col_params = self._column.to_sql()
        return (
            f"{col_sql} FUZZY_MATCH(?, ?)",
            col_params + (self._pattern, self._threshold),
        )
```

## 添加运算符支持

表达式类通过组合 `rhosocial.activerecord.backend.expression.mixins` 中的 mixin 获得运算符：

| Mixin | 提供 |
|-------|------|
| `ComparisonMixin` | `==`、`!=`、`>`、`>=`、`<`、`<=`、`is_null()`、`in_()`、`between()` |
| `ArithmeticMixin` | `+`、`-`、`*`、`/`、`%` |
| `LogicalMixin` | `&` (AND)、`\|` (OR)、`~` (NOT) |
| `StringMixin` | `.like()`、`.ilike()` |
| `AliasableMixin` | `.as_()` 别名 |
| `TypeCastingMixin` | `.cast()` |

带运算符支持的示例：

```python
from rhosocial.activerecord.backend.expression.bases import SQLValueExpression, SQLQueryAndParams
from rhosocial.activerecord.backend.expression.mixins import ArithmeticMixin, ComparisonMixin


class DistanceExpression(ArithmeticMixin, ComparisonMixin, SQLValueExpression):
    """自定义表达式：ST_Distance(col1, col2)。"""

    def __init__(self, dialect, col1, col2):
        super().__init__(dialect)
        self._col1 = col1
        self._col2 = col2

    def to_sql(self) -> SQLQueryAndParams:
        sql1, params1 = self._col1.to_sql()
        sql2, params2 = self._col2.to_sql()
        return f"ST_Distance({sql1}, {sql2})", params1 + params2
```

现在您可以使用比较运算符：

```python
# 查找 10 公里内的用户
distance = DistanceExpression(dialect, User.c.location, Literal(dialect, target_location))
query = User.query().where(distance < 10000)
```

## 委托给 Dialect

推荐的模式是将 SQL 格式化委托给 dialect，而不是硬编码 SQL 字符串。这允许不同的后端生成不同的 SQL：

```python
class MyCustomExpression(SQLValueExpression):
    def to_sql(self) -> SQLQueryAndParams:
        # 委托给 dialect 进行后端特定的格式化
        return self.dialect.format_my_custom_expression(self._inner)
```

然后在您的自定义 dialect mixin 中：

```python
class MyCustomDialectMixin:
    def format_my_custom_expression(self, expr):
        sql, params = expr.inner.to_sql()
        # PostgreSQL 语法
        return f"MY_FUNC({sql})", params
```

## 在查询中使用

### 直接构造

```python
expr = RegexMatchExpression(dialect, User.c.name, r'^admin.*')
query = User.query().where(expr)
```

### 作为模型方法（推荐）

将表达式包装在模型方法中以获得更清晰的 API：

```python
class User(ActiveRecord):
    name: str

    @classmethod
    def table_name(cls) -> str:
        return 'users'

    @classmethod
    def regex_match(cls, column, pattern):
        """创建 REGEXP 匹配表达式。"""
        return RegexMatchExpression(cls.backend().dialect, column, pattern)

# 用法
query = User.query().where(User.regex_match(User.c.name, r'^admin.*'))
```

## 另请参阅

- [PostgreSQL Dialect 表达式](../backend_specific_features/dialect.md) — 表达式系统架构
- [自定义数据类型](custom_types.md) — 定义新的 DataType 子类
- [核心：表达式基类](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/backend/expression)

💡 *AI 提示词：* "如何为数据库特定函数创建自定义表达式？"
