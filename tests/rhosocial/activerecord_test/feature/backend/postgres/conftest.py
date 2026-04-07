# tests/rhosocial/activerecord_test/feature/backend/postgres/conftest.py
"""Fixtures for PostgreSQL transaction expression tests."""
import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


@pytest.fixture(scope="function")
def postgres_dialect():
    """Fixture providing PostgresDialect instance for testing transaction expressions."""
    return PostgresDialect()
