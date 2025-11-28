---
name: Bug Report
about: Report a bug in rhosocial ActiveRecord PostgreSQL Backend
title: '[POSTGRES-BUG] '
labels: 'bug, postgres'
assignees: ''
---

## Before Submitting

Please ensure this bug is specific to the PostgreSQL backend implementation. If the issue occurs across all backends (involving ActiveRecord/ActiveQuery functionality), please submit the issue at https://github.com/rhosocial/python-activerecord/issues instead.

## Description

A clear and concise description of the bug in the PostgreSQL backend.

## Environment

- **rhosocial ActiveRecord PostgreSQL Version**: [e.g. 1.0.0.dev13]
- **rhosocial ActiveRecord Core Version**: [e.g. 1.0.0.dev13]
- **Python Version**: [e.g. 3.13]
- **PostgreSQL Version**: [e.g. 15.x, 14.x, 13.x]
- **psycopg Version**: [e.g. psycopg 3.2.0]
- **OS**: [e.g. Linux, macOS, Windows]

## Steps to Reproduce

1.
2.
3.

## Expected Behavior

A clear and concise description of what you expected to happen.

## Actual Behavior

What actually happened instead of the expected behavior.

## Database Query

If applicable, provide the generated SQL query that causes the issue:

```sql
-- Your problematic SQL query here
```

## Model Definition

If the issue is related to a specific model, please share your model definition:

```python
# Example model definition
class User(ActiveRecord):
    __table_name__ = 'users'

    id: Optional[int] = None
    name: str
    email: EmailStr
```

## PostgreSQL-Specific Configuration

Any PostgreSQL-specific configuration that might be relevant:

```python
config = PostgreSQLConnectionConfig(
    host='localhost',
    port=5432,
    database='test',
    username='user',
    password='password',
    # Add your specific options here
)
```

## PostgreSQL Features Used

Which PostgreSQL-specific features are involved in this bug? (Select all that apply)

- [ ] JSON/JSONB operations
- [ ] Array operations
- [ ] Window functions
- [ ] Common Table Expressions (CTEs)
- [ ] Recursive CTEs
- [ ] Full-text search
- [ ] Geometric types
- [ ] Network address types
- [ ] Range types
- [ ] UUID type
- [ ] Custom types

## Error Details

If you're getting an error, include the full error message and stack trace:

```
Paste the full error message here
```

## Additional Context

Any other context about the problem here.