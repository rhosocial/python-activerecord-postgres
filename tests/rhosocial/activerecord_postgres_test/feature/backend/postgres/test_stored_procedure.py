# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_stored_procedure.py
"""Unit tests for PostgreSQL stored procedure mixin.

Tests for:
- PostgresStoredProcedureMixin feature detection
- Format CREATE PROCEDURE statement
- Format DROP PROCEDURE statement
- Format CALL statement
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class TestStoredProcedureFeatureDetection:
    """Test stored procedure feature detection methods."""

    def test_supports_call_statement_pg10(self):
        """PostgreSQL 10 does not support CALL statement."""
        dialect = PostgresDialect((10, 0, 0))
        assert dialect.supports_call_statement() is False

    def test_supports_call_statement_pg11(self):
        """PostgreSQL 11 supports CALL statement."""
        dialect = PostgresDialect((11, 0, 0))
        assert dialect.supports_call_statement() is True

    def test_supports_stored_procedure_transaction_control_pg10(self):
        """PostgreSQL 10 does not support transaction control in procedures."""
        dialect = PostgresDialect((10, 0, 0))
        assert dialect.supports_stored_procedure_transaction_control() is False

    def test_supports_stored_procedure_transaction_control_pg11(self):
        """PostgreSQL 11 supports transaction control in procedures."""
        dialect = PostgresDialect((11, 0, 0))
        assert dialect.supports_stored_procedure_transaction_control() is True

    def test_supports_sql_body_functions_pg13(self):
        """PostgreSQL 13 does not support SQL-body functions."""
        dialect = PostgresDialect((13, 0, 0))
        assert dialect.supports_sql_body_functions() is False

    def test_supports_sql_body_functions_pg14(self):
        """PostgreSQL 14 supports SQL-body functions."""
        dialect = PostgresDialect((14, 0, 0))
        assert dialect.supports_sql_body_functions() is True


class TestFormatCreateProcedureStatement:
    """Test CREATE PROCEDURE statement formatting."""

    def test_create_procedure_pg10_raises_error(self):
        """CREATE PROCEDURE should raise error on PostgreSQL 10."""
        dialect = PostgresDialect((10, 0, 0))
        with pytest.raises(ValueError, match="requires PostgreSQL 11"):
            dialect.format_create_procedure_statement(
                name="test_proc",
                body="BEGIN NULL; END"
            )

    def test_create_procedure_basic(self):
        """Test basic CREATE PROCEDURE statement."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_create_procedure_statement(
            name="test_proc",
            body="BEGIN NULL; END"
        )
        assert "CREATE PROCEDURE \"test_proc\"" in sql
        assert "LANGUAGE plpgsql" in sql
        assert "BEGIN NULL; END" in sql
        assert params == ()

    def test_create_procedure_with_schema(self):
        """Test CREATE PROCEDURE with schema."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_create_procedure_statement(
            name="test_proc",
            schema="public",
            body="BEGIN NULL; END"
        )
        assert '"public"."test_proc"' in sql

    def test_create_procedure_with_or_replace(self):
        """Test CREATE OR REPLACE PROCEDURE statement."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_create_procedure_statement(
            name="test_proc",
            or_replace=True,
            body="BEGIN NULL; END"
        )
        assert "CREATE OR REPLACE PROCEDURE" in sql

    def test_create_procedure_with_parameters(self):
        """Test CREATE PROCEDURE with parameters."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_create_procedure_statement(
            name="test_proc",
            parameters=[
                {"name": "p_id", "type": "integer"},
                {"name": "p_name", "type": "varchar", "mode": "IN"},
                {"name": "p_result", "type": "integer", "mode": "OUT"},
            ],
            body="BEGIN p_result := p_id; END"
        )
        assert "p_id integer" in sql
        assert "p_name varchar" in sql
        assert "OUT p_result integer" in sql

    def test_create_procedure_with_default_values(self):
        """Test CREATE PROCEDURE with default parameter values."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_create_procedure_statement(
            name="test_proc",
            parameters=[
                {"name": "p_id", "type": "integer", "default": "0"},
                {"name": "p_name", "type": "varchar", "default": "'unknown'"},
            ],
            body="BEGIN NULL; END"
        )
        assert "p_id integer = 0" in sql
        assert "p_name varchar = 'unknown'" in sql

    def test_create_procedure_with_security_definer(self):
        """Test CREATE PROCEDURE with SECURITY DEFINER."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_create_procedure_statement(
            name="test_proc",
            security="DEFINER",
            body="BEGIN NULL; END"
        )
        assert "SECURITY DEFINER" in sql

    def test_create_procedure_with_security_invoker(self):
        """Test CREATE PROCEDURE with SECURITY INVOKER."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_create_procedure_statement(
            name="test_proc",
            security="INVOKER",
            body="BEGIN NULL; END"
        )
        assert "SECURITY INVOKER" in sql

    def test_create_procedure_invalid_security_raises_error(self):
        """Test CREATE PROCEDURE with invalid security option."""
        dialect = PostgresDialect((14, 0, 0))
        with pytest.raises(ValueError, match="Security must be"):
            dialect.format_create_procedure_statement(
                name="test_proc",
                security="INVALID",
                body="BEGIN NULL; END"
            )

    def test_create_procedure_with_set_params(self):
        """Test CREATE PROCEDURE with SET parameters."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_create_procedure_statement(
            name="test_proc",
            set_params={"work_mem": "256MB", "effective_cache_size": "1GB"},
            body="BEGIN NULL; END"
        )
        assert "SET work_mem = 256MB" in sql
        assert "effective_cache_size = 1GB" in sql

    def test_create_procedure_with_language(self):
        """Test CREATE PROCEDURE with custom language."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_create_procedure_statement(
            name="test_proc",
            language="sql",
            body="SELECT 1"
        )
        assert "LANGUAGE sql" in sql


class TestFormatDropProcedureStatement:
    """Test DROP PROCEDURE statement formatting."""

    def test_drop_procedure_basic(self):
        """Test basic DROP PROCEDURE statement."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_drop_procedure_statement(
            name="test_proc"
        )
        assert sql == "DROP PROCEDURE \"test_proc\""
        assert params == ()

    def test_drop_procedure_with_schema(self):
        """Test DROP PROCEDURE with schema."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_drop_procedure_statement(
            name="test_proc",
            schema="public"
        )
        assert 'DROP PROCEDURE "public"."test_proc"' in sql

    def test_drop_procedure_if_exists(self):
        """Test DROP PROCEDURE IF EXISTS."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_drop_procedure_statement(
            name="test_proc",
            if_exists=True
        )
        assert "DROP PROCEDURE IF EXISTS \"test_proc\"" in sql

    def test_drop_procedure_cascade(self):
        """Test DROP PROCEDURE CASCADE."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_drop_procedure_statement(
            name="test_proc",
            cascade=True
        )
        assert "DROP PROCEDURE \"test_proc\" CASCADE" in sql

    def test_drop_procedure_with_parameter_types(self):
        """Test DROP PROCEDURE with parameter types for overload resolution."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_drop_procedure_statement(
            name="test_proc",
            parameters=[
                {"type": "integer"},
                {"type": "varchar"}
            ]
        )
        assert '"test_proc"(integer, varchar)' in sql

    def test_drop_procedure_with_parameter_type_strings(self):
        """Test DROP PROCEDURE with parameter types as strings."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_drop_procedure_statement(
            name="test_proc",
            parameters=["integer", "varchar"]
        )
        assert '"test_proc"(integer, varchar)' in sql


class TestFormatCallStatement:
    """Test CALL statement formatting."""

    def test_call_statement_pg10_raises_error(self):
        """CALL statement should raise error on PostgreSQL 10."""
        dialect = PostgresDialect((10, 0, 0))
        with pytest.raises(ValueError, match="requires PostgreSQL 11"):
            dialect.format_call_statement(name="test_proc")

    def test_call_statement_no_args(self):
        """Test CALL statement without arguments."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_call_statement(name="test_proc")
        assert sql == 'CALL "test_proc"()'
        assert params == ()

    def test_call_statement_with_args(self):
        """Test CALL statement with arguments."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_call_statement(
            name="test_proc",
            arguments=[1, "test", None]
        )
        assert 'CALL "test_proc"(%s, %s, %s)' in sql
        assert params == (1, "test", None)

    def test_call_statement_with_schema(self):
        """Test CALL statement with schema."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_call_statement(
            name="test_proc",
            schema="public"
        )
        assert 'CALL "public"."test_proc"()' in sql
