# types tests

PostgreSQL type layer: pure-function tests for PostgresTypeFormatSupportMixin formatting/parsing and the PostgreSQL-specific DataType subclasses (synonyms registration, equality/hash semantics, array-type equivalence).

## Key files

- `test_data_type_formatting.py` — DataType formatting and parsing
- `test_expression_types.py` — PG-specific DataType semantics
