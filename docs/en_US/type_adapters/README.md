# Type Adapters

Type adapters handle conversion between Python and PostgreSQL types.

## Topics

- **[PostgreSQL to Python Type Mapping](./mapping.md)**: Type conversion rules
- **[Custom Type Adapters](./custom.md)**: Extending type support
- **[Timezone Handling](./timezone.md)**: TIMESTAMP WITH TIME ZONE
- **[Array Type Handling](./arrays.md)**: PostgreSQL array support

## Built-in Adapters

| Python Type | PostgreSQL Type |
|-------------|-----------------|
| `str` | `TEXT`, `VARCHAR` |
| `int` | `INTEGER`, `BIGINT` |
| `float` | `REAL`, `DOUBLE PRECISION` |
| `bool` | `BOOLEAN` |
| `bytes` | `BYTEA` |
| `date` | `DATE` |
| `time` | `TIME` |
| `datetime` | `TIMESTAMP` |
| `UUID` | `UUID` |
| `dict` | `JSONB` |
| `list` | `ARRAY` |

💡 *AI Prompt:* "How do type adapters handle NULL values?"
