# PostgreSQL-Specific Field Types

## Array Types

PostgreSQL natively supports arrays:

```python
from rhosocial.activerecord.model import ActiveRecord

class Article(ActiveRecord):
    __table_name__ = "articles"
    title: str
    tags: list        # TEXT[]

# Create with array
article = Article(
    title="PostgreSQL Arrays",
    tags=["database", "postgres", "arrays"]
)
article.save()

# Query arrays
articles = Article.query().where(
    "? = ANY(tags)", ("postgres",)
).all()
```

## JSONB Type

JSONB provides binary JSON storage with indexing support:

```python
class Product(ActiveRecord):
    __table_name__ = "products"
    name: str
    attributes: dict    # JSONB

product = Product(
    name="Laptop",
    attributes={
        "brand": "Dell",
        "specs": {"cpu": "Intel i7", "ram": "16GB"}
    }
)
product.save()

# JSONB queries
products = Product.query().where(
    "attributes->>'brand' = ?", ("Dell",)
).all()

# JSONB contains (@>)
products = Product.query().where(
    "attributes @> ?", ('{"brand": "Dell"}',)
).all()
```

## UUID Type

PostgreSQL has native UUID support:

```python
from uuid import UUID, uuid4

class User(ActiveRecord):
    __table_name__ = "users"
    id: UUID
    name: str

user = User(id=uuid4(), name="John")
user.save()
```

## Range Types

PostgreSQL supports range types:

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

# Query ranges
bookings = Booking.query().where(
    "date_range @> ?", (date(2024, 1, 3),)
).all()
```

💡 *AI Prompt:* "What are the performance implications of using JSONB versus separate tables?"
