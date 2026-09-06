# 自定义数据类型

## 概述

rhosocial-activerecord 的类型系统是可扩展的。您可以为类型层次结构未覆盖的数据库特定列类型创建自定义 DataType 子类。

**关键原则**：数据类型是表达式。它们继承自 `BaseExpression`，遵循相同的委托模式 — `to_sql()` 委托给 dialect 的 `format_data_type()` 方法。这意味着 DataType 与其他表达式享有相同的可扩展性、序列化支持和 dialect 委托。

## DataType 基类

所有数据类型继承自 `rhosocial.activerecord.backend.expression.types._base` 中的 `DataType`。DataType 实例是**值对象** — 两个相同类型的实例比较相等，与 dialect 绑定无关。

生命周期：**声明 → 绑定 → 渲染**

1. **声明** — 无 dialect 构造（模型字段声明、迁移）
2. **绑定** — 通过构造函数、`bind()` 或 `parse_type()` 工厂附加 dialect
3. **渲染** — `to_sql()` 委托给 `dialect.format_data_type(self)`

## 创建简单 DataType

用于无参数的类型：

```python
from rhosocial.activerecord.backend.expression.types._base import DataType


class PostGISPointType(DataType):
    """使用 PostGIS 的自定义地理点类型。"""
    pass
```

## 创建带参数的 DataType

用于接受参数的类型（如精度、长度等）：

```python
from rhosocial.activerecord.backend.expression.types._base import DataType
from typing import Optional


class PostgresVectorType(DataType):
    """PostgreSQL VECTOR(n) — pgvector 扩展。"""
    dim: int

    def __init__(self, dialect=None, *, dim: int):
        super().__init__(dialect)
        self.dim = dim

    def __eq__(self, other: object) -> bool:
        if type(self) is not type(other):
            return False
        return self.dim == other.dim

    def __hash__(self) -> int:
        return hash((type(self), self.dim))
```

**重要**：带参数的类型必须实现 `__eq__` 和 `__hash__`。比较基于类型的逻辑参数，而不是 dialect 绑定。

## 扩展现有类型

要创建现有类型的后端特定变体：

```python
from rhosocial.activerecord.backend.expression.types import IntegerType


class PostgresSerialType(IntegerType):
    """PostgreSQL SERIAL / BIGSERIAL。"""
    big: bool = False

    def __init__(self, dialect=None, *, big: bool = False):
        super().__init__(dialect)
        self.big = big

    def __eq__(self, other: object) -> bool:
        if type(self) is not type(other):
            return False
        return self.big == other.big

    def __hash__(self) -> int:
        return hash((type(self), self.big))

    @classmethod
    def synonyms(cls) -> set:
        """标记为与 IntegerType 等效以进行 schema 比较。"""
        return {'IntegerType'}
```

`synonyms()` 方法标记类型在 schema 比较目的上结构等效。

## 注册类型格式化器

要将自定义类型渲染为 SQL，使用 `@handles()` 装饰器注册格式化器：

```python
from rhosocial.activerecord.backend.dialect.mixins.ddl_type import DDLTypeMixin


class MyTypeSupportMixin(DDLTypeMixin):
    """添加自定义类型格式化的 mixin。"""

    @DDLTypeMixin.handles(PostgresVectorType)
    def format_data_type_vector(self, data_type: PostgresVectorType):
        return f"vector({data_type.dim})", ()

    @DDLTypeMixin.handles(PostgresSerialType)
    def format_data_type_serial(self, data_type: PostgresSerialType):
        sql = "BIGSERIAL" if data_type.big else "SERIAL"
        return sql, ()
```

然后将此 mixin 组合到您的 dialect 中：

```python
class MyCustomDialect(PostgresDialect, MyTypeSupportMixin):
    pass
```

`@handles()` 装饰器确保当 `dialect.format_data_type()` 遇到您的自定义类型时调用您的格式化器。

## 在模型中使用自定义类型

### 直接字段声明

```python
class Product(ActiveRecord):
    embedding: list  # 将使用后端的 array/VECTOR 类型

    @classmethod
    def table_name(cls) -> str:
        return 'products'
```

### 使用 UseSqlType 显式类型注解

要获得更多控制，使用 `UseSqlType` 指定确切的 SQL 类型：

```python
from rhosocial.activerecord.base.fields import UseSqlType

class Product(ActiveRecord):
    embedding: UseSqlType[PostgresVectorType] = UseSqlType(dim=128)

    @classmethod
    def table_name(cls) -> str:
        return 'products'
```

## 运行时检查类型支持

使用协议系统检查类型是否受支持：

```python
from rhosocial.activerecord.backend.dialect.protocols import JSONSupport, ArraySupport

dialect = backend.dialect

if isinstance(dialect, JSONSupport) and dialect.supports_json_type():
    # JSON 类型可用
    ...

if isinstance(dialect, ArraySupport) and dialect.supports_array_type():
    # 数组类型可用
    ...
```

## 另请参阅

- [PostgreSQL 字段类型](../backend_specific_features/field_types.md) — 核心 DataType 层次结构
- [自定义类型适配器](custom_adapters.md) — Python 到数据库值转换
- [类型适配器](../type_adapters/README.md) — 类型转换系统
- [核心：自定义类型](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/modeling/custom_types)

💡 *AI 提示词：* "如何添加对数据库特定列类型的支持？"
