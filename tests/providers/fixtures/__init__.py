# tests/providers/fixtures/__init__.py
"""PostgreSQL-specific DDL expression fixtures for the testsuite providers.

Each module in this package exposes a ``TABLE_EXPRESSIONS`` mapping of
table name -> factory callable ``Callable[[DialectLike, str], CreateTableExpression]``.

The factory functions build :class:`CreateTableExpression` instances that emit
PostgreSQL-compatible DDL (SERIAL / UUID / JSONB / BYTEA / TIMESTAMPTZ / BOOLEAN /
DECIMAL / VARCHAR / TEXT / FK constraints / CHECK / UNIQUE / NOT NULL / DEFAULT).
The pre-existing ``.sql`` schema files under
``tests/rhosocial/activerecord_postgres_test/feature/<feature>/schema/``
remain as the authoritative reference for what the expressions here must
produce; they are simply no longer read at runtime by the providers.
"""
