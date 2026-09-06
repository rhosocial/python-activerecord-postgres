# RETURNING Clause

PostgreSQL's RETURNING clause returns data from modified rows.

## INSERT RETURNING

```python
user = User(name="John", email="john@example.com")
user.save()

# The user object now contains all generated values
print(user.id)        # Auto-generated
print(user.created_at)  # Default value
```

## UPDATE RETURNING

```python
# Update with returning
result = User.query().where(
    User.c.id == 1
).update(name="Jane")

# Returns updated rows
```

## DELETE RETURNING

```python
# Delete with returning
result = User.query().where(
    User.c.id == 1
).delete()

# Returns deleted rows
```

## Use Cases

1. **Get auto-generated IDs**: Retrieve serial/identity values after INSERT
2. **Audit changes**: Log the actual values that were modified
3. **Cascade info**: Get related data before deletion

💡 *AI Prompt:* "How does RETURNING improve efficiency compared to separate SELECT queries?"
