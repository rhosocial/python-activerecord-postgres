# 数组类型对比：PostgreSQL 与其他数据库

## 概述

PostgreSQL 在常用数据库中独树一帜，提供原生数组类型支持。本文档解释 PostgreSQL 的数组能力与不支持原生数组的数据库之间的关键差异。

## 原生数组支持

### PostgreSQL

PostgreSQL 支持任意内置或用户定义类型的数组：

```sql
-- 原生数组列
CREATE TABLE articles (
    id SERIAL PRIMARY KEY,
    tags TEXT[],           -- 文本数组
    scores INTEGER[],      -- 整数数组
    uuids UUID[]           -- UUID 数组
);
```

**模型定义（无需适配器）：**

```python
from rhosocial.activerecord.model import ActiveRecord
from typing import List, Optional

class Article(ActiveRecord):
    __table_name__ = "articles"
    
    title: str
    tags: Optional[List[str]] = None      # 直接映射到 TEXT[]
    scores: Optional[List[int]] = None    # 直接映射到 INTEGER[]
```

### 不支持原生数组的数据库

许多数据库没有原生数组类型。在这种情况下，列表必须存储为序列化字符串：

```sql
-- 使用 TEXT 存储序列化格式
CREATE TABLE articles (
    id INTEGER PRIMARY KEY,
    tags TEXT,    -- 序列化列表："tag1,tag2,tag3"
    scores TEXT   -- 序列化列表："1,2,3"
);
```

**模型定义（需要适配器）：**

```python
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.backend.type_adapter import BaseSQLTypeAdapter
from rhosocial.activerecord.base.fields import UseAdapter
from typing import List, Optional

class ListToStringAdapter(BaseSQLTypeAdapter):
    """将 Python 列表转换为逗号分隔的字符串。"""
    
    def __init__(self):
        super().__init__()
        self._register_type(list, str)

    def _do_to_database(self, value: List, target_type: type, options=None) -> Optional[str]:
        if value is None:
            return None
        return ",".join(str(v) for v in value)

    def _do_from_database(self, value: str, target_type: type, options=None) -> Optional[List]:
        if value is None:
            return None
        return value.split(",") if value else []

class Article(ActiveRecord):
    __table_name__ = "articles"
    
    title: str
    tags: Optional[List[str]] = None  # 需要序列化适配器
```

## 关键差异

| 特性 | PostgreSQL | 不支持原生数组的数据库 |
|------|------------|----------------------|
| 原生数组类型 | 支持（`TEXT[]`、`INTEGER[]` 等） | 不支持 |
| 需要类型适配器 | 否 | 是 |
| 数组操作符 | `@>`、`<@`、`&&`、`ANY()`、`ALL()` | 不可用 |
| 索引支持 | GIN 索引 | 不适用 |
| 多维数组 | 支持（`INTEGER[][]`） | 不支持 |
| 查询性能 | 可通过索引优化 | 需使用 LIKE 或字符串函数 |

## PostgreSQL 数组操作符

PostgreSQL 提供强大的数组专用操作符，这些在不支持原生数组的数据库中不可用：

```python
# 包含：tags 包含 'python'
Article.query().where("tags @> ARRAY[?]", ('python',)).all()

# 包含多个：tags 同时包含 'python' 和 'database'
Article.query().where("tags @> ARRAY[?, ?]", ('python', 'database')).all()

# 重叠：tags 包含以下任意一个
Article.query().where("tags && ARRAY[?, ?]", ('python', 'database')).all()

# 任意元素等于
Article.query().where("? = ANY(tags)", ('python',)).all()

# 所有元素满足条件
Article.query().where("? = ALL(tags)", ('python',)).all()

# 数组长度
Article.query().where("array_length(tags, 1) > ?", (3,)).all()
```

**不支持数组的数据库等效写法（功能受限）：**

```python
# 必须使用字符串匹配（效率较低，精度较低）
Article.query().where("tags LIKE ?", ('%python%')).all()
```

## 编写可移植代码

### 方案一：后端特定模型

为 PostgreSQL 和其他后端分别定义模型：

```python
# 通用版本（用于不支持原生数组的数据库）
class MixedAnnotationModel(ActiveRecord):
    tags: Annotated[List[str], UseAdapter(ListToStringAdapter(), str)]

# PostgreSQL 特定版本
class PostgresMixedAnnotationModel(ActiveRecord):
    tags: Optional[List[str]] = None  # 原生数组，无需适配器
```

### 方案二：使用 JSON 数组

为保持跨数据库可移植性，使用 JSON 数组：

```python
# 大多数数据库支持 JSON
class Article(ActiveRecord):
    __table_name__ = "articles"
    tags: Optional[List[str]] = None  # 存储为 JSON
```

### 方案三：特性检测

运行时检测数组支持：

```python
from rhosocial.activerecord.backend.interface import SupportsArrayQueries

if isinstance(model.__backend__, SupportsArrayQueries):
    # 使用原生数组操作符
    results = model.query().where("tags @> ARRAY[?]", ('python',)).all()
else:
    # 降级为字符串匹配
    results = model.query().where("tags LIKE ?", ('%python%')).all()
```

## Schema 对比示例

### PostgreSQL Schema

```sql
-- 原生数组支持
CREATE TABLE "mixed_annotation_items" (
    "id" SERIAL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "tags" TEXT[],           -- 原生数组
    "meta" TEXT,
    "description" TEXT,
    "status" TEXT
);
```

### 不支持原生数组的数据库 Schema

```sql
-- 必须使用 TEXT 配合序列化
CREATE TABLE "mixed_annotation_items" (
    "id" INTEGER PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "tags" TEXT,             -- 序列化字符串
    "meta" TEXT,
    "description" TEXT,
    "status" TEXT
);
```

## 性能考量

### PostgreSQL 数组

- **优点**：GIN 索引支持、高效查询、类型安全
- **缺点**：PostgreSQL 专属特性、小数组存储空间较大

### 序列化字符串

- **优点**：跨数据库可移植、Schema 简单
- **缺点**：无法索引、字符串解析开销、查询能力有限

## 建议

1. **仅 PostgreSQL 项目**：使用原生数组以获得更好的性能和查询能力
2. **多数据库项目**：使用 JSON 数组以保持可移植性
3. **简单字符串列表**：序列化适配器在所有后端都可接受

💡 *AI 提示词：* "如何在 PostgreSQL 数组列上创建 GIN 索引？"
