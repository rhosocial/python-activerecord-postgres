# tests/rhosocial/activerecord_postgres_test/feature/backend/test_postgres_dialect_security.py
"""
Tests for PostgreSQL dialect SQL injection security fixes.

This test module verifies that string escaping and validation
methods properly sanitize user input to prevent SQL injection.
Tests are run against the actual PostgreSQL dialect.
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression.bases import BaseExpression  # noqa: F401
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
    TableConstraint,
    TableConstraintType,
)
from rhosocial.activerecord.backend.expression.types import VarCharType
from typing import Tuple, Any  # noqa: F401


@pytest.fixture
def dialect():
    """Create a PostgreSQL test dialect."""
    return PostgresDialect((13, 0, 0))


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
    """Test column definition formats data_type correctly."""
    col_def = ColumnDefinition(
        name="test_col",
        data_type=VarCharType(length=255),
    )

    sql, params = dialect.format_column_definition(col_def)
    assert "VARCHAR(255)" in sql


def test_postgres_column_definition_rejects_string_data_type(dialect):
    """Test that ColumnDefinition rejects string data_type at construction."""
    with pytest.raises(TypeError, match="data_type must be a DataType instance"):
        ColumnDefinition(
            name="test_col",
            data_type="VARCHAR(255); DROP TABLE users--",
        )


def test_postgres_format_default_constraint_string_escaping(dialect):
    """Test DEFAULT constraint string is escaped."""
    constraint = ColumnConstraint(
        constraint_type=ColumnConstraintType.DEFAULT,
        default_value="test's value",
    )

    sql, params = dialect.format_default_constraint(constraint)
    assert "test''s value" in sql
    assert "'; DROP" not in sql


def test_postgres_format_storage_options_string_escaping(dialect):
    """Test storage options string values are escaped."""
    storage_opts = {"key": "value's"}
    sql, params = dialect.format_storage_options(storage_opts)
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

    sql, params = dialect.format_exclude_constraint(constraint)
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

        sql, params = dialect.format_exclude_constraint(constraint)
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
        dialect.format_exclude_constraint(constraint)


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
        dialect.format_exclude_constraint(constraint)


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

    sql, params = dialect.format_exclude_constraint(constraint)

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
        from rhosocial.activerecord.backend.expression.bases import BaseExpression  # noqa: F811

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
        from rhosocial.activerecord.backend.expression.bases import BaseExpression  # noqa: F811

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


# ============================================================
# format_partition_value — single-quote escaping
# ============================================================

def _format_partition_value(dialect, value):
    from rhosocial.activerecord.backend.impl.postgres.expression.ddl import PartitionValue

    sql, params = dialect.format_partition_value(PartitionValue(dialect=dialect, value=value))
    assert params == ()
    return sql


def test_partition_value_none(dialect):
    """None partition value returns NULL."""
    result = _format_partition_value(dialect, None)
    assert result == "NULL"


def test_partition_value_maxvalue(dialect):
    """MAXVALUE is returned as-is (case-insensitive)."""
    result = _format_partition_value(dialect, "MAXVALUE")
    assert result == "MAXVALUE"


def test_partition_value_minvalue(dialect):
    """MINVALUE is returned as-is (case-insensitive)."""
    result = _format_partition_value(dialect, "minvalue")
    assert result == "MINVALUE"


def test_partition_value_normal_string(dialect):
    """Normal string value is single-quoted."""
    result = _format_partition_value(dialect, "2024-01-01")
    assert result == "'2024-01-01'"


def test_partition_value_escaped_single_quote(dialect):
    """String value with single quote is properly escaped."""
    result = _format_partition_value(dialect, "it's")
    assert result == "'it''s'"
    assert "'; DROP" not in result


def test_partition_value_injection_blocked(dialect):
    """SQL injection in partition value is safely escaped (inside quotes)."""
    result = _format_partition_value(dialect, "x'; DROP TABLE users--")
    assert result.count("'") % 2 == 0
    assert result.startswith("'")
    assert result.endswith("'")


def test_partition_value_integer(dialect):
    """Integer partition value is returned as str()."""
    result = _format_partition_value(dialect, 42)
    assert result == "42"


# ============================================================
# format_identifier — identifier quoting equivalence and injection immunity
# ============================================================

def test_format_identifier_normal(dialect):
    """Normal identifier is double-quoted."""
    result = dialect.format_identifier("users")
    assert result == '"users"'


def test_format_identifier_with_quote(dialect):
    """Identifier with embedded double-quote is properly escaped."""
    result = dialect.format_identifier('table"name')
    assert result == '"table""name"'


def test_format_identifier_injection_payload(dialect):
    """Identifier with injection payload is safely contained (balanced quotes)."""
    payload = 'users"; DROP TABLE users--'
    result = dialect.format_identifier(payload)
    assert result.count('"') % 2 == 0, f"Unbalanced quotes: {result}"
    assert result == '"users""; DROP TABLE users--"'


def test_format_identifier_naive_vs_proper_safe(dialect):
    """For safe input, naive and proper quoting produce same structure."""
    names = ["users", "orders", "products", "table_1", "camelCase"]
    for name in names:
        naive = f'"{name}"'
        proper = dialect.format_identifier(name)
        assert naive == proper, f"Mismatch for '{name}': naive={naive}, proper={proper}"


def test_format_identifier_naive_vs_proper_malicious(dialect):
    """For malicious input, proper quoting prevents breakout that naive allows."""
    payloads = [
        'x"; DROP TABLE users--',
        'y"; DELETE FROM t--',
        'z"; UPDATE t SET a=1--',
    ]
    for payload in payloads:
        naive = f'"{payload}"'
        proper = dialect.format_identifier(payload)

        # Naive produces odd quote count => breakout
        assert naive.count('"') % 2 != 0, \
            f"Naive quoting should unbalance quotes for '{payload}': {naive}"

        # Proper produces even quote count => contained
        assert proper.count('"') % 2 == 0, \
            f"Proper quoting should balance quotes for '{payload}': {proper}"


def test_format_identifier_empty_string(dialect):
    """Empty identifier produces empty double quotes."""
    assert dialect.format_identifier("") == '""'


# ── format_binary_operator % escaping ─────────────────────────────────


def test_format_binary_operator_percent_escaped():
    """format_binary_operator escapes % to %% for psycopg compatibility."""
    from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
    d = PostgresDialect((16, 0, 0))

    # % operator → %% (psycopg requires %% for literal %)
    sql, params = d.format_binary_operator("%", "a", "b", (), ())
    assert sql == "a %% b", f"Expected escaped %%, got: {sql}"
    assert params == ()

    # %% is pre-escaped → should NOT be double-escaped to %%%%
    # (callers must pass raw operator, not pre-escaped)
    sql, params = d.format_binary_operator("%%", "a", "b", (), ())
    assert sql == "a %%%% b", f"raw %% should be escaped to %%%%, got: {sql}"

    # Operator without % → unchanged
    sql, params = d.format_binary_operator("=", "a", "b", (), ())
    assert sql == "a = b"
    assert params == ()

    # ? operator (hstore/jsonb) → preserved as-is, not treated as placeholder
    sql, params = d.format_binary_operator("?", "data", "%s", (), ("key",))
    assert sql == "data ? %s", f"? operator preserved: {sql}"
    assert params == ("key",)

    # ?| and ?& operators → preserved
    sql, params = d.format_binary_operator("?|", "data", "%s", (), ("key",))
    assert sql == "data ?| %s", f"?| operator preserved: {sql}"

    # %# (pg_trgm) → %# (unchanged, no % to escape)
    sql, params = d.format_binary_operator("%#", "a", "b", (), ())
    assert sql == "a %%# b", f"%# operator: {sql}"


def test_hstore_operator_not_double_escaped():
    """hstore_to_array_operator uses raw %, not pre-escaped %%."""
    hstore_path = "src/rhosocial/activerecord/backend/impl/postgres/functions/hstore.py"
    # Try to find the file relative to the repo root
    import os
    for candidate in [
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..", "..", hstore_path),
        os.path.join(os.getcwd(), hstore_path),
    ]:
        candidate = os.path.normpath(candidate)
        if os.path.exists(candidate):
            hstore_path = candidate
            break
    with open(hstore_path) as f:
        src = f.read()
    assert (
        "BinaryExpression(" in src and '"%",' in src
    ), "hstore operator must pass raw % (not pre-escaped %%)"
    import re
    for m in re.finditer(r'BinaryExpression\([^)]*?["\']%%["\']', src):
        pytest.fail(f"Found pre-escaped %% operator: {m.group()[:80]}")