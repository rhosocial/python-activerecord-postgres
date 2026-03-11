# 类型适配器

类型适配器处理 Python 和 PostgreSQL 类型之间的转换。

## 主题

- **[PostgreSQL 到 Python 类型映射](./mapping.md)**: 类型转换规则
- **[自定义类型适配器](./custom.md)**: 扩展类型支持
- **[时区处理](./timezone.md)**: TIMESTAMP WITH TIME ZONE
- **[数组类型处理](./arrays.md)**: PostgreSQL 数组支持
- **[数组类型对比](./array_comparison.md)**: PostgreSQL 与 MySQL/SQLite 数组处理对比

## 适配器注册状态

后端在初始化时会自动注册部分类型适配器。了解哪些适配器已注册、哪些需要手动注册，有助于正确使用 PostgreSQL 特有类型。

### 默认注册的适配器

以下适配器在后端初始化时自动注册，可直接使用：

| Python 类型 | PostgreSQL 类型 | 适配器 | 说明 |
|------------|----------------|--------|------|
| `dict` | `JSONB` | `PostgresJSONBAdapter` | 自动序列化/反序列化 |
| `list` | `ARRAY` | `PostgresListAdapter` | 支持多维数组 |
| `Point`, `Line` 等 | 几何类型 | `PostgresGeometryAdapter` | 点、线、多边形等 |
| `PostgresMoney` | `MONEY` | `PostgresMoneyAdapter` | 货币类型 |
| `PostgresMacaddr` | `MACADDR` | `PostgresMacaddrAdapter` | MAC 地址 |
| `PostgresTsVector` | `TSVECTOR` | `PostgresTsVectorAdapter` | 全文搜索向量 |
| `PostgresTsQuery` | `TSQUERY` | `PostgresTsQueryAdapter` | 全文搜索查询 |
| `OID`, `RegClass` 等 | 对象标识符 | `PostgresOidAdapter` | OID 和注册表类型 |
| `XID`, `TID` 等 | 事务标识符 | `PostgresXidAdapter` / `PostgresTidAdapter` | 事务和元组标识符 |
| `PostgresLsn` | `PG_LSN` | `PostgresLsnAdapter` | 日志序列号 |
| `Enum` | `ENUM` | `PostgresEnumAdapter` | 枚举类型 |

### 不默认注册的适配器

以下适配器因存在类型冲突或需要用户决策，不自动注册：

| Python 类型 | PostgreSQL 类型 | 适配器 | 原因 |
|------------|----------------|--------|------|
| `PostgresRange` | 范围类型 | `PostgresRangeAdapter` | 用户可选择使用 psycopg 原生 `Range` |
| `PostgresMultirange` | 多范围类型 | `PostgresMultirangeAdapter` | 同上，且需要 PostgreSQL 14+ |
| `PostgresXML` | `XML` | `PostgresXMLAdapter` | str→str 映射冲突 |
| `PostgresBitString` | `BIT`, `VARBIT` | `PostgresBitStringAdapter` | str→str 映射冲突 |
| `PostgresJsonPath` | `JSONPATH` | `PostgresJsonPathAdapter` | str→str 映射冲突 |

## 使用建议

### 原生类型

对于 Python 原生类型，直接使用即可，无需额外配置：

```python
from rhosocial.activerecord import ActiveRecord
from datetime import datetime
from decimal import Decimal
from uuid import UUID

class User(ActiveRecord):
    name: str           # VARCHAR/TEXT
    age: int            # INTEGER
    score: float        # REAL/DOUBLE PRECISION
    active: bool        # BOOLEAN
    data: dict          # JSON/JSONB
    tags: list          # ARRAY
    created_at: datetime  # TIMESTAMP
    balance: Decimal    # NUMERIC
    id: UUID            # UUID
```

### PostgreSQL 特有类型（已注册）

对于已注册适配器的 PostgreSQL 特有类型，使用对应的包装类：

```python
from rhosocial.activerecord.backend.impl.postgres import (
    PostgresMoney, Point, PostgresTsVector
)

class Order(ActiveRecord):
    total: PostgresMoney      # MONEY 类型
    location: Point           # POINT 类型
    search_vector: PostgresTsVector  # TSVECTOR 类型
```

### Range 类型（可选注册）

Range 类型有两种使用方式：

**方式一：使用 psycopg 原生 Range（推荐纯后端使用）**

```python
from psycopg.types.range import Range

# 直接传递 psycopg Range，无转换开销
backend.execute(
    "SELECT * FROM orders WHERE period @> %s",
    [Range(1, 10, '[)')]
)
```

**方式二：使用 PostgresRange 包装器（推荐 ActiveRecord 模型）**

```python
from rhosocial.activerecord.backend.impl.postgres import (
    PostgresRange, PostgresRangeAdapter
)

# 手动注册适配器
backend.adapter_registry.register(PostgresRangeAdapter(), PostgresRange, str)

class Order(ActiveRecord):
    period: PostgresRange  # 范围类型
```

### 冲突类型（需显式注册）

对于存在 str→str 映射冲突的类型，必须显式注册适配器：

```python
from rhosocial.activerecord.backend.impl.postgres import (
    PostgresXML, PostgresXMLAdapter,
    PostgresBitString, PostgresBitStringAdapter,
    PostgresJsonPath, PostgresJsonPathAdapter
)

# 方式一：在 backend 上全局注册
backend.adapter_registry.register(PostgresXMLAdapter(), PostgresXML, str)
backend.adapter_registry.register(PostgresBitStringAdapter(), PostgresBitString, str)
backend.adapter_registry.register(PostgresJsonPathAdapter(), PostgresJsonPath, str)

# 方式二：在查询时指定（推荐）
result = backend.execute(
    "INSERT INTO docs (content) VALUES (%s)",
    [PostgresXML("<root>data</root>")],
    type_adapters={PostgresXML: PostgresXMLAdapter()}
)
```

### 在 ActiveRecord 模型中使用冲突类型

```python
from rhosocial.activerecord import ActiveRecord
from rhosocial.activerecord.backend.impl.postgres import (
    PostgresXML, PostgresXMLAdapter,
    PostgresBitString, PostgresBitStringAdapter
)

class Document(ActiveRecord):
    content: PostgresXML      # 需要显式注册适配器
    flags: PostgresBitString  # 需要显式注册适配器

# 配置模型时注册适配器
Document.configure(
    backend=backend,
    type_adapters={
        PostgresXML: PostgresXMLAdapter(),
        PostgresBitString: PostgresBitStringAdapter(),
    }
)
```

## 内置适配器总览

| Python 类型 | PostgreSQL 类型 | 适配器 | 注册状态 |
|------------|----------------|--------|----------|
| `str` | `TEXT`, `VARCHAR` | 基础适配器 | 始终可用 |
| `int` | `INTEGER`, `BIGINT` | 基础适配器 | 始终可用 |
| `float` | `REAL`, `DOUBLE PRECISION` | 基础适配器 | 始终可用 |
| `bool` | `BOOLEAN` | 基础适配器 | 始终可用 |
| `bytes` | `BYTEA` | 基础适配器 | 始终可用 |
| `date` | `DATE` | 基础适配器 | 始终可用 |
| `time` | `TIME` | 基础适配器 | 始终可用 |
| `datetime` | `TIMESTAMP` | 基础适配器 | 始终可用 |
| `UUID` | `UUID` | 基础适配器 | 始终可用 |
| `dict` | `JSONB` | `PostgresJSONBAdapter` | ✅ 默认注册 |
| `list` | `ARRAY` | `PostgresListAdapter` | ✅ 默认注册 |
| 几何类型 | `POINT`, `LINE` 等 | `PostgresGeometryAdapter` | ✅ 默认注册 |
| `PostgresMoney` | `MONEY` | `PostgresMoneyAdapter` | ✅ 默认注册 |
| `PostgresMacaddr` | `MACADDR` | `PostgresMacaddrAdapter` | ✅ 默认注册 |
| `PostgresTsVector` | `TSVECTOR` | `PostgresTsVectorAdapter` | ✅ 默认注册 |
| `PostgresRange` | 范围类型 | `PostgresRangeAdapter` | ❓ 可选 |
| `PostgresXML` | `XML` | `PostgresXMLAdapter` | ⚠️ 需显式注册 |
| `PostgresBitString` | `BIT`, `VARBIT` | `PostgresBitStringAdapter` | ⚠️ 需显式注册 |
| `PostgresJsonPath` | `JSONPATH` | `PostgresJsonPathAdapter` | ⚠️ 需显式注册 |

💡 *AI 提示词：* "类型适配器如何处理 NULL 值？"
