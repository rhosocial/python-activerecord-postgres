# 自定义 (Customization)

## 概述

rhosocial-activerecord 设计为可扩展的。您可以在多个层次上自定义框架：

1. **自定义表达式** — 为数据库特定 SQL 语法创建新的表达式类
2. **自定义数据类型** — 为自定义列类型定义新的 DataType 子类
3. **自定义类型适配器** — 注册 Python 对象和数据库值之间的转换器

## 自定义架构

框架使用**委托 + 组合**模式：

- **表达式**通过委托给其绑定的 dialect 的 `format_*()` 方法来生成 SQL
- **数据类型**通过委托给 `dialect.format_data_type()` 来生成 DDL SQL
- **Dialect** 由小的、专注的 mixin 组合而成（每个 mixin 为一个功能区域提供 `format_*()` 方法）
- **类型适配器**在 `TypeRegistry` 中注册，用于转换 Python 值和数据库值

这意味着您可以在不修改核心库的情况下扩展任何层。

## 何时自定义

| 需求 | 方法 |
|------|------|
| 数据库有表达式库中没有的函数 | 自定义表达式 |
| 数据库有类型系统中没有的列类型 | 自定义数据类型 |
| Python 对象需要自定义序列化 | 自定义类型适配器 |

## 内容

- [自定义表达式](custom_expressions.md)：创建新的表达式类
- [自定义数据类型](custom_types.md)：定义新的 DataType 子类
- [自定义类型适配器](custom_adapters.md)：注册自定义转换器

## 另请参阅

- [PostgreSQL Dialect 表达式](../backend_specific_features/dialect.md) — 表达式系统架构
- [PostgreSQL 字段类型](../backend_specific_features/field_types.md) — DataType 层次结构
- [类型适配器](../type_adapters/README.md) — 类型转换系统

💡 *AI 提示词：* "如何添加表达式库中没有的数据库特定函数的支持？"
