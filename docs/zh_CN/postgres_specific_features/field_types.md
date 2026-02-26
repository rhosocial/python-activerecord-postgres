# PostgreSQL 特定字段类型

## 数组类型

PostgreSQL 原生支持数组：

```python
from rhosocial.activerecord.model import ActiveRecord

class Article(ActiveRecord):
    __table_name__ = "articles"
    title: str
    tags: list        # TEXT[]

# 创建带有数组的记录
article = Article(
    title="PostgreSQL 数组",
    tags=["database", "postgres", "arrays"]
)
article.save()

# 查询数组
articles = Article.query().where(
    "? = ANY(tags)", ("postgres",)
).all()
```

## JSONB 类型

JSONB 提供二进制 JSON 存储，支持索引：

```python
class Product(ActiveRecord):
    __table_name__ = "products"
    name: str
    attributes: dict    # JSONB

product = Product(
    name="笔记本电脑",
    attributes={
        "brand": "Dell",
        "specs": {"cpu": "Intel i7", "ram": "16GB"}
    }
)
product.save()

# JSONB 查询
products = Product.query().where(
    "attributes->>'brand' = ?", ("Dell",)
).all()

# JSONB 包含 (@>)
products = Product.query().where(
    "attributes @> ?", ('{"brand": "Dell"}',)
).all()
```

## UUID 类型

PostgreSQL 原生支持 UUID：

```python
from uuid import UUID, uuid4

class User(ActiveRecord):
    __table_name__ = "users"
    id: UUID
    name: str

user = User(id=uuid4(), name="张三")
user.save()
```

## 范围类型

PostgreSQL 支持范围类型：

```python
from datetime import date

class Booking(ActiveRecord):
    __table_name__ = "bookings"
    room_id: int
    date_range: tuple    # DATERANGE

booking = Booking(
    room_id=101,
    date_range=(date(2024, 1, 1), date(2024, 1, 7))
)
booking.save()

# 查询范围
bookings = Booking.query().where(
    "date_range @> ?", (date(2024, 1, 3),)
).all()
```

💡 *AI 提示词：* "使用 JSONB 与使用单独的表有什么性能影响？"
