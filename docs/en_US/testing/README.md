# Testing

This section covers testing for the PostgreSQL backend.

## Contents

- [Test Configuration](configuration.md): test environment setup
- [Local PostgreSQL Testing](local.md): local database testing

## Testing Principles

### Sync/Async Parity

All backends with IO operations must prepare **paired sync and async tests** for equivalent scenarios:

```python
# Sync test
def test_create_user():
    user = User(name="Alice").create()
    assert user.id is not None

# Async test — same logic, async API
async def test_async_create_user():
    user = await AsyncUser(name="Alice").create()
    assert user.id is not None
```

If a backend only supports sync or only async, prepare the corresponding tests only.

### Expression Classes — No IO

Expression tests involve no database IO — they only build SQL and validate the generated SQL:

```python
def test_expression_sql():
    expr = Eq(User.name, "Alice")
    assert expr.to_sql(dialect) == "`name` = %s"
    assert expr.params == ["Alice"]
```

No async counterpart is needed for expression tests.

### ActiveRecord Tests — Use Testsuite

ActiveRecord feature tests (model CRUD, relationships, queries) use the **testsuite**:

```
python-activerecord-testsuite/
└── src/rhosocial/activerecord/testsuite/feature/
    ├── basic/      # Level 1 — must pass first
    ├── relation/   # Level 1 — must pass first
    ├── query/      # Level 1 — must pass first
    ├── events/     # Level 2 — extended behaviors
    ├── mixins/     # Level 2
    ├── interface/  # Level 2
    └── examples/   # Level 2
```

Each backend provides **provider implementations** that wire the tests to its specific database. The test logic is shared; only the provider layer changes per backend.

**Running testsuite tests:**

```bash
cd python-activerecord-postgres
PYTHONPATH=tests .venv3.14-ubuntu26.04/bin/pytest \
    ../python-activerecord-testsuite/src/rhosocial/activerecord/testsuite/feature/relation/
```

**Provider responsibilities:** See the [Core Testsuite Provider Guide](provider_guide.md).

### Test Categories Summary

| What to Test | Approach | IO? | Async? |
|-------------|----------|-----|--------|
| Expression classes (dialect SQL generation) | Unit tests, no DB | No | No |
| Type adapters (type conversion) | Unit tests, no DB | No | No |
| Named features (connection, expression, procedure, migration) | Backend CLI scripts | Yes | If supported |
| ActiveRecord features (CRUD, relations, queries) | Testsuite + provider | Yes | Yes |
| Backend-specific features (unique types, syntax) | Project-specific tests | Yes | Yes |

## Provider Responsibilities

For provider implementation guidelines, see the [Core Testsuite Provider Guide](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/testing/provider_guide.md).

### PostgreSQL-Specific Notes

- **Custom types**: PostgreSQL custom types (e.g., ENUM, ARRAY) must be dropped before tables
- **Cleanup order**: DROP TABLE → Drop custom types → Close cursors → Disconnect

### Implementation Reference

See the test suite documentation for detailed implementation guidelines:
- `python-activerecord-testsuite/docs/en_US/README.md`
- [Core Backend Testing Guide](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/testing/backend_testing.md)
