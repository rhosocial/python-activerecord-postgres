# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/ddl/test_database_ddl.py
"""Explicit PostgresDialect database DDL capability + rendering tests."""

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.objects import Database
from rhosocial.activerecord.backend.expression.statements.ddl_database import (
    CreateDatabaseStatement,
    DropDatabaseStatement,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


def _dialect(version=(15, 0, 0)):
    return PostgresDialect(version=version)


def test_database_capabilities():
    dialect = _dialect()
    assert dialect.supports_create_database() is True
    assert dialect.supports_drop_database() is True


def test_database_if_not_exists_has_no_clause():
    """CREATE DATABASE has no IF NOT EXISTS in any PostgreSQL version."""
    for version in ((9, 6, 0), (13, 0, 0), (18, 0, 0)):
        assert _dialect(version).supports_database_if_not_exists() is False


def test_database_if_exists_is_in_the_baseline_synopsis():
    """DROP DATABASE IF EXISTS is present from the 9.6 baseline onward."""
    for version in ((9, 6, 0), (12, 0, 0), (18, 0, 0)):
        assert _dialect(version).supports_database_if_exists() is True


def test_database_force_drop_is_version_gated():
    """WITH (FORCE) arrived in PostgreSQL 13."""
    assert _dialect((9, 6, 0)).supports_database_force_drop() is False
    assert _dialect((12, 0, 0)).supports_database_force_drop() is False
    assert _dialect((13, 0, 0)).supports_database_force_drop() is True


def test_create_database_renders():
    dialect = _dialect()
    sql, params = CreateDatabaseStatement(dialect, Database(dialect, "app")).to_sql()
    assert "CREATE DATABASE" in sql
    assert params == ()


def test_create_database_if_not_exists_refused():
    dialect = _dialect()
    expr = CreateDatabaseStatement(
        dialect, Database(dialect, "app"), if_not_exists=True
    )
    with pytest.raises(UnsupportedFeatureError, match="CREATE DATABASE IF NOT EXISTS"):
        expr.to_sql()


def test_drop_database_renders():
    dialect = _dialect()
    sql, _ = DropDatabaseStatement(dialect, Database(dialect, "app")).to_sql()
    assert "DROP DATABASE" in sql


def test_drop_database_if_exists_renders_below_thirteen():
    """The clause was silently dropped on 9.6-12 before the probe was fixed."""
    dialect = _dialect((12, 0, 0))
    sql, _ = DropDatabaseStatement(
        dialect, Database(dialect, "app"), if_exists=True
    ).to_sql()
    assert sql == 'DROP DATABASE IF EXISTS "app"'


def test_drop_database_force_refused_before_thirteen():
    dialect = _dialect((12, 0, 0))
    expr = DropDatabaseStatement(dialect, Database(dialect, "app"), force=True)
    with pytest.raises(UnsupportedFeatureError, match="WITH \\(FORCE\\)"):
        expr.to_sql()


def test_drop_database_force_renders_from_thirteen():
    dialect = _dialect((13, 0, 0))
    sql, _ = DropDatabaseStatement(
        dialect, Database(dialect, "app"), if_exists=True, force=True
    ).to_sql()
    assert sql == 'DROP DATABASE IF EXISTS "app" WITH (FORCE)'
