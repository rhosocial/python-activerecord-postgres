# tests/rhosocial/activerecord_postgres_test/feature/backend/test_generated_column_capabilities.py
"""Explicit PostgreSQL generated-column capability assertions.

The generic ``GeneratedColumnExpression``/formatter already covers these
features; this pins the capability bits and their version gate.
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


def test_generated_columns_gated_on_pg12():
    assert PostgresDialect(version=(12, 0, 0)).supports_generated_columns() is True
    assert PostgresDialect(version=(11, 0, 0)).supports_generated_columns() is False


def test_stored_generated_columns_follow_generated_columns():
    assert PostgresDialect(version=(12, 0, 0)).supports_stored_generated_columns() is True


def test_virtual_generated_columns_unsupported():
    assert PostgresDialect(version=(15, 0, 0)).supports_virtual_generated_columns() is False
