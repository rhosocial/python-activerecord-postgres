# tests/rhosocial/activerecord_postgres_test/feature/backend/expression/test_expression_signatures.py
"""SQL-snapshot tests for format-signature-compliant expression classes.

These tests build expressions with a bare ``PostgresDialect`` (no DB
connection) and assert on the generated SQL, so they run without a server.
"""
import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.core import Column
from rhosocial.activerecord.backend.impl.postgres.expression.ilike import (
    ILIKEExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.type import (
    EnumTypeNameExpression,
    EnumValuesExpression,
    CreateEnumTypeExpression,
    DropEnumTypeExpression,
    AlterEnumAddValueExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.multirange import (
    CreateMultirangeTypeExpression,
    MultirangeAggFunctionExpression,
)


class TestILIKEExpression:
    """Tests for ILIKEExpression."""

    def test_basic_ilike(self, postgres_dialect):
        expr = ILIKEExpression(
            postgres_dialect,
            column=Column(postgres_dialect, "name"),
            pattern="%foo%",
        )
        sql, params = expr.to_sql()
        assert sql == '"name" ILIKE %s'
        assert params == ('%foo%',)

    def test_negated_ilike(self, postgres_dialect):
        expr = ILIKEExpression(
            postgres_dialect,
            column=Column(postgres_dialect, "name"),
            pattern="%bar%",
            negate=True,
        )
        sql, params = expr.to_sql()
        assert sql == '"name" NOT ILIKE %s'
        assert params == ('%bar%',)

    def test_ilike_dispatches_to_postgres_format(self, postgres_dialect):
        """Verify ILIKEExpression.to_sql() dispatches to format_ilike_expression."""
        expr = ILIKEExpression(
            postgres_dialect,
            column=Column(postgres_dialect, "col"),
            pattern="pat",
        )
        sql, params = expr.to_sql()
        assert "ILIKE" in sql
        assert params == ("pat",)

    def test_ilike_generates_native_ilike_not_lower(self, postgres_dialect):
        """Verify generated SQL uses native ILIKE, not LOWER() LIKE LOWER()."""
        expr = ILIKEExpression(
            postgres_dialect,
            column=Column(postgres_dialect, "c"),
            pattern="%x%",
        )
        sql, _ = expr.to_sql()
        assert "LOWER" not in sql.upper()
        assert "ILIKE" in sql


class TestEnumTypeNameExpression:
    """Tests for EnumTypeNameExpression."""

    def test_name_without_schema(self, postgres_dialect):
        expr = EnumTypeNameExpression(postgres_dialect, name="status")
        sql, params = expr.to_sql()
        assert sql == '"status"'
        assert params == ()

    def test_name_with_schema(self, postgres_dialect):
        expr = EnumTypeNameExpression(postgres_dialect, name="status", schema="app")
        sql, params = expr.to_sql()
        assert sql == '"app"."status"'
        assert params == ()


class TestEnumValuesExpression:
    """Tests for EnumValuesExpression."""

    def test_single_value(self, postgres_dialect):
        expr = EnumValuesExpression(postgres_dialect, values=["active"])
        sql, params = expr.to_sql()
        assert sql == "'active'"
        assert params == ()

    def test_multiple_values(self, postgres_dialect):
        expr = EnumValuesExpression(postgres_dialect, values=["a", "b", "c"])
        sql, params = expr.to_sql()
        assert sql == "'a', 'b', 'c'"
        assert params == ()


class TestCreateEnumTypeExpression:
    """Tests for CreateEnumTypeExpression."""

    def test_basic_create(self, postgres_dialect):
        expr = CreateEnumTypeExpression(
            postgres_dialect, name="status", values=["active", "inactive"],
        )
        sql, params = expr.to_sql()
        assert sql == "CREATE TYPE \"status\" AS ENUM ('active', 'inactive')"
        assert params == ()

    def test_create_if_not_exists_fails_fast(self, postgres_dialect):
        expr = CreateEnumTypeExpression(
            postgres_dialect,
            name="priority",
            values=["low", "high"],
            schema="app",
            if_not_exists=True,
        )
        with pytest.raises(UnsupportedFeatureError, match="CREATE TYPE IF NOT EXISTS"):
            expr.to_sql()


class TestDropEnumTypeExpression:
    """Tests for DropEnumTypeExpression."""

    def test_basic_drop(self, postgres_dialect):
        expr = DropEnumTypeExpression(postgres_dialect, name="status")
        sql, params = expr.to_sql()
        assert sql == "DROP TYPE \"status\""
        assert params == ()

    def test_drop_with_if_exists_and_cascade(self, postgres_dialect):
        expr = DropEnumTypeExpression(
            postgres_dialect, name="status", schema="app",
            if_exists=True, cascade=True,
        )
        sql, params = expr.to_sql()
        assert sql == "DROP TYPE IF EXISTS \"app\".\"status\" CASCADE"
        assert params == ()


class TestAlterEnumAddValueExpression:
    """Tests for AlterEnumAddValueExpression."""

    def test_basic_add_value(self, postgres_dialect):
        expr = AlterEnumAddValueExpression(
            postgres_dialect, type_name="status", new_value="archived",
        )
        sql, params = expr.to_sql()
        assert sql == "ALTER TYPE \"status\" ADD VALUE 'archived'"
        assert params == ()

    def test_add_value_with_before_after(self, postgres_dialect):
        expr = AlterEnumAddValueExpression(
            postgres_dialect, type_name="status", new_value="pending",
            schema="app", before="active",
        )
        sql, params = expr.to_sql()
        assert sql == "ALTER TYPE \"app\".\"status\" ADD VALUE 'pending' BEFORE 'active'"
        assert params == ()


class TestCreateMultirangeTypeExpression:
    def test_explicit_multirange_ddl_fails_fast(self, postgres_dialect):
        expr = CreateMultirangeTypeExpression(
            postgres_dialect,
            name="my_multirange",
            range_type="my_range",
        )
        with pytest.raises(UnsupportedFeatureError, match="CREATE TYPE AS MULTIRANGE"):
            expr.to_sql()


class TestMultirangeAggFunctionExpression:
    """Tests for MultirangeAggFunctionExpression."""

    def test_basic_agg(self, postgres_dialect):
        expr = MultirangeAggFunctionExpression(
            postgres_dialect, range_column="period", table_name="events",
        )
        sql, params = expr.to_sql()
        assert sql == 'SELECT multirange_agg("period") FROM "events"'
        assert params == ()

    def test_agg_with_where_and_schema(self, postgres_dialect):
        expr = MultirangeAggFunctionExpression(
            postgres_dialect, range_column="period", table_name="events",
            where_clause="status = 'active'", schema="public",
        )
        sql, params = expr.to_sql()
        assert sql == "SELECT multirange_agg(\"period\") FROM \"public\".\"events\" WHERE status = 'active'"
        assert params == ()
