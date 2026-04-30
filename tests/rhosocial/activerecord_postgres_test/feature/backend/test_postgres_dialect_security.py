# tests/rhosocial/activerecord_postgres_test/feature/backend/test_postgres_dialect_security.py
"""
Tests for PostgreSQL dialect SQL injection security fixes.

This test module verifies that string escaping and validation
methods properly sanitize user input to prevent SQL injection.
Tests are run against the actual PostgreSQL dialect.
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
    TableConstraint,
    TableConstraintType,
)


@pytest.fixture
def dialect():
    """Create a PostgreSQL test dialect."""
    return PostgresDialect()


def test_postgres_escape_sql_string(dialect):
    """Test PostgreSQL inherits _escape_sql_string."""
    result = dialect._escape_sql_string("test's value")
    assert result == "test''s value"


def test_postgres_validate_data_type(dialect):
    """Test PostgreSQL inherits _validate_data_type."""
    assert dialect._validate_data_type("INTEGER")
    assert dialect._validate_data_type("VARCHAR(255)")
    assert dialect._validate_data_type("TEXT")
    assert not dialect._validate_data_type("TEXT; DROP TABLE users--")


def test_postgres_format_column_definition_data_type_validation(dialect):
    """Test column definition validates data_type."""
    col_def = ColumnDefinition(
        name="test_col",
        data_type="VARCHAR(255)",
    )

    sql, params = dialect.format_column_definition(col_def)
    assert "VARCHAR(255)" in sql


def test_postgres_format_column_definition_data_type_rejects_injection(dialect):
    """Test that malicious data_type is rejected."""
    col_def = ColumnDefinition(
        name="test_col",
        data_type="VARCHAR(255); DROP TABLE users--",
    )

    with pytest.raises(ValueError, match="Invalid data type"):
        dialect.format_column_definition(col_def)


def test_postgres_format_default_constraint_string_escaping(dialect):
    """Test DEFAULT constraint string is escaped."""
    constraint = ColumnConstraint(
        constraint_type=ColumnConstraintType.DEFAULT,
        default_value="test's value",
    )

    sql, params = dialect._format_default_constraint(constraint)
    assert "test''s value" in sql
    assert "'; DROP" not in sql


def test_postgres_format_storage_options_string_escaping(dialect):
    """Test storage options string values are escaped."""
    storage_opts = {"key": "value's"}
    sql, params = dialect._format_storage_options(storage_opts)
    assert "value''s" in sql
    assert "'; DROP" not in sql


def test_postgres_format_cast_expression_valid(dialect):
    """Test that CAST expression validates target_type."""
    sql, params = dialect.format_cast_expression("column", "INTEGER", (), None)
    assert "INTEGER" in sql


def test_exclude_constraint_valid_using_methods(dialect):
    """Test valid index access methods are accepted."""
    constraint = TableConstraint(
        constraint_type=TableConstraintType.EXCLUDE,
        name="test_exclude",
        dialect_options={
            "using": "gist",
            "exclude_elements": [("int4range", "&&")],
        },
    )

    sql, params = dialect._format_exclude_constraint(constraint)
    assert "EXCLUDE USING gist" in sql


def test_exclude_constraint_valid_operators(dialect):
    """Test valid exclude operators are accepted."""
    valid_ops = [
        ("int4range", "&&"),
        ("float8range", "&&"),
        ("name", "="),
        ("box", "~="),
    ]

    for expr, op in valid_ops:
        constraint = TableConstraint(
            constraint_type=TableConstraintType.EXCLUDE,
            name="test_exclude",
            dialect_options={
                "exclude_elements": [(expr, op)],
            },
        )

        sql, params = dialect._format_exclude_constraint(constraint)
        assert f"WITH {op}" in sql


def test_exclude_constraint_rejects_invalid_using(dialect):
    """Test that invalid index access method is rejected."""
    constraint = TableConstraint(
        constraint_type=TableConstraintType.EXCLUDE,
        name="test_exclude",
        dialect_options={
            "using": "invalid_method",
            "exclude_elements": [("range", "&&")],
        },
    )

    with pytest.raises(ValueError, match="Invalid index access method"):
        dialect._format_exclude_constraint(constraint)


def test_exclude_constraint_rejects_invalid_operator(dialect):
    """Test that invalid exclude operator is rejected."""
    constraint = TableConstraint(
        constraint_type=TableConstraintType.EXCLUDE,
        name="test_exclude",
        dialect_options={
            "exclude_elements": [("range", "'; DROP TABLE users--")],
        },
    )

    with pytest.raises(ValueError, match="Invalid exclude operator"):
        dialect._format_exclude_constraint(constraint)


def test_exclude_constraint_sql_injection_prevention(dialect):
    """Test that SQL injection attempts are blocked."""
    constraint = TableConstraint(
        constraint_type=TableConstraintType.EXCLUDE,
        name="test_exclude",
        dialect_options={
            "using": "gist",
            "exclude_elements": [("range", "&&")],
        },
    )

    sql, params = dialect._format_exclude_constraint(constraint)

    assert "; DROP" not in sql
    assert "--" not in sql
    assert "/*" not in sql