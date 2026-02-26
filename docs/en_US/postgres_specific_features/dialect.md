# PostgreSQL Dialect Expressions

## RETURNING Clause

PostgreSQL supports RETURNING for DML operations:

```python
# INSERT RETURNING
user = User(name="John")
user.save()
# Returns the inserted row with generated id

# UPDATE RETURNING
User.query().where(User.c.id == 1).update(name="Jane")
# Returns affected rows
```

## DISTINCT ON

PostgreSQL-specific DISTINCT ON:

```python
# Get the most recent order for each user
orders = Order.query().distinct_on("user_id").order_by(
    "user_id", "created_at DESC"
).all()
```

## ILIKE (Case-Insensitive Match)

```python
users = User.query().where(
    "name ILIKE ?", ("%john%",)
).all()
```

## Array Operators

```python
# Contains (@>)
Article.query().where("tags @> ?", (['python', 'database'],))

# Is contained by (<@)
Article.query().where("tags <@ ?", (['python', 'database', 'web'],))

# Overlaps (&&)
Article.query().where("tags && ?", (['python', 'java'],))

# Any element
Article.query().where("? = ANY(tags)", ('python',))
```

## JSONB Operators

```python
# Get JSON value at path
Product.query().where("attributes->>'brand' = ?", ('Dell',))

# Get nested value
Product.query().where("attributes->'specs'->>'cpu' = ?", ('Intel i7',))

# JSONB contains
Product.query().where("attributes @> ?", ({"brand": "Dell"},))

# Key exists
Product.query().where("attributes ? 'brand'", ())
```

💡 *AI Prompt:* "How do PostgreSQL's ILIKE and standard LIKE differ in performance?"
