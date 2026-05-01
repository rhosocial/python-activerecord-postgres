# tests/rhosocial/activerecord_postgres_test/feature/backend/test_postgres_dialect_security.py
"""
Tests for PostgreSQL dialect SQL injection security fixes.

This test module verifies that string escaping and validation
methods properly sanitize user input to prevent SQL injection.
Tests are run against the actual PostgreSQL dialect.
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
    TableConstraint,
    TableConstraintType,
)
from typing import Tuple, Any


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


class TestPostgresEnumSecurity:
    """Tests for CREATE TYPE ENUM value escaping (fix for SQL injection risk)."""

    def test_create_type_enum_values_escaped(self, dialect):
        """Test ENUM values are properly escaped with single quote doubling."""
        sql, params = dialect.format_create_type_enum_statement(
            name="test_enum",
            values=["it's", "normal", "test's value"],
        )

        assert "it''s" in sql
        assert "test''s value" in sql
        assert "'; DROP" not in sql

    def test_create_type_enum_sql_injection_blocked(self, dialect):
        """Test SQL injection in ENUM values is properly escaped (not executed)."""
        sql, params = dialect.format_create_type_enum_statement(
            name="test_enum",
            values=["normal", "'; DROP TABLE users--"],
        )

        assert "DROP TABLE users--" in sql
        assert "CREATE TYPE" in sql
        assert "AS ENUM" in sql


class TestPostgresStoredProcedureNameSecurity:
    """Tests for stored procedure names using format_identifier."""

    def test_create_procedure_name_quoted(self, dialect):
        """Test CREATE PROCEDURE name is properly quoted."""
        sql, params = dialect.format_create_procedure_statement(
            name="my_procedure",
            body="BEGIN END",
            language="PL/pgSQL",
        )

        assert '"my_procedure"' in sql

    def test_drop_procedure_name_quoted(self, dialect):
        """Test DROP PROCEDURE name is properly quoted."""
        sql, params = dialect.format_drop_procedure_statement(
            name="my_procedure",
        )

        assert '"my_procedure"' in sql

    def test_call_statement_name_quoted(self, dialect):
        """Test CALL statement procedure name is properly quoted."""
        sql, params = dialect.format_call_statement(
            name="my_procedure",
        )

        assert '"my_procedure"' in sql

    def test_procedure_with_schema_quoted(self, dialect):
        """Test procedure with schema is properly quoted."""
        sql, params = dialect.format_create_procedure_statement(
            schema="my_schema",
            name="my_procedure",
            body="BEGIN END",
            language="PL/pgSQL",
        )

        assert '"my_schema"' in sql
        assert '"my_procedure"' in sql


class TestPostgresPartialIndexWhereClauseSecurity:
    """Tests for partial index WHERE clause (ToSQLProtocol support)."""

    def test_create_index_with_string_where_clause(self, dialect):
        """Test CREATE INDEX with string WHERE clause (backward compatible)."""
        sql, params = dialect.format_create_index_pg_statement(
            index_name="idx_test",
            table_name="users",
            columns=["email"],
            where_clause="active = true",
        )

        assert "WHERE active = true" in sql
        assert params == ()

    def test_create_index_with_to_sql_protocol(self, dialect):
        """Test CREATE INDEX with ToSQLProtocol expression (parameterized)."""
        from rhosocial.activerecord.backend.expression.bases import BaseExpression

        class MockWhereExpr(BaseExpression):
            def __init__(self):
                self._sql = "age >= 18"
                self._params = (18,)

            def to_sql(self):
                return self._sql, self._params

        sql, params = dialect.format_create_index_pg_statement(
            index_name="idx_active",
            table_name="users",
            columns=["email"],
            where_clause=MockWhereExpr(),
        )

        assert "WHERE age >= 18" in sql
        assert params == (18,)

    def test_create_index_where_clause_params_collected(self, dialect):
        """Test WHERE clause parameters are properly collected and returned."""
        from rhosocial.activerecord.backend.expression.bases import BaseExpression

        class MockWhereExpr(BaseExpression):
            def __init__(self):
                self._sql = "status = %s"
                self._params = ("active",)

            def to_sql(self):
                return self._sql, self._params

        sql, params = dialect.format_create_index_pg_statement(
            index_name="idx_test",
            table_name="users",
            columns=["status"],
            where_clause=MockWhereExpr(),
        )

        assert "WHERE status = %s" in sql
        assert params == ("active",)


class TestPostgresTriggerFunctionNameSecurity:
    """Tests for trigger function name using format_identifier."""

    def test_trigger_function_name_quoted(self, dialect):
        """Test trigger function name is properly quoted with double quotes."""
        from rhosocial.activerecord.backend.expression.statements.ddl_trigger import (
            CreateTriggerExpression,
            TriggerTiming,
            TriggerEvent,
        )

        expr = CreateTriggerExpression(
            dialect=dialect,
            trigger_name="my_trigger",
            table_name="users",
            timing=TriggerTiming.BEFORE,
            events=[TriggerEvent.INSERT],
            function_name="my_function",
        )

        sql, params = dialect.format_create_trigger_statement(expr)

        assert '"my_function"' in sql

    def test_trigger_function_name_special_chars_quoted(self, dialect):
        """Test trigger function name with special characters is quoted."""
        from rhosocial.activerecord.backend.expression.statements.ddl_trigger import (
            CreateTriggerExpression,
            TriggerTiming,
            TriggerEvent,
        )

        expr = CreateTriggerExpression(
            dialect=dialect,
            trigger_name="trigger",
            table_name="users",
            timing=TriggerTiming.BEFORE,
            events=[TriggerEvent.INSERT],
            function_name="Function With Spaces",
        )

        sql, params = dialect.format_create_trigger_statement(expr)

        assert '"Function With Spaces"' in sql


class TestPostgresExtendedStatisticsNameSecurity:
    """Tests for extended statistics names using format_identifier."""

    def test_create_statistics_name_quoted(self, dialect):
        """Test CREATE STATISTICS name is properly quoted."""
        from rhosocial.activerecord.backend.impl.postgres.expression.ddl.statistics import (
            PostgresCreateStatisticsExpression,
        )

        expr = PostgresCreateStatisticsExpression(
            dialect=dialect,
            name="my_stats",
            table_name="users",
            columns=["email"],
            statistics_type="ndistinct",
        )

        sql, params = dialect.format_create_statistics_statement(expr)

        assert '"my_stats"' in sql

    def test_drop_statistics_name_quoted(self, dialect):
        """Test DROP STATISTICS name is properly quoted."""
        from rhosocial.activerecord.backend.impl.postgres.expression.ddl.statistics import (
            PostgresDropStatisticsExpression,
        )

        expr = PostgresDropStatisticsExpression(
            dialect=dialect,
            name="my_stats",
        )

        sql, params = dialect.format_drop_statistics_statement(expr)

        assert '"my_stats"' in sql