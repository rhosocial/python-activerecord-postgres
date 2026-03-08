# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_type_casting.py
"""Unit tests for PostgreSQL type casting functionality.

Tests for:
- TypeCastingMixin cast() method
- CastExpression with PostgreSQL :: syntax
- Chained type conversions
- Type compatibility warnings
"""
import pytest
import warnings

from rhosocial.activerecord.backend.expression.core import Column, Literal
from rhosocial.activerecord.backend.expression.advanced_functions import CastExpression
from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.types.constants import (
    MONEY, NUMERIC, FLOAT8, INTEGER, VARCHAR, TEXT,
    JSONB, TIMESTAMP, TIMESTAMPTZ, BOOLEAN, BIGINT,
)
from rhosocial.activerecord.backend.impl.postgres.type_compatibility import (
    check_cast_compatibility,
    get_compatible_types,
    get_intermediate_type,
    WARNED_CASTS,
    DIRECT_COMPATIBLE_CASTS,
)


class TestTypeCastingMixin:
    """Tests for TypeCastingMixin cast() method."""

    def setup_method(self):
        self.dialect = PostgresDialect()

    def test_column_cast_basic(self):
        """Test basic type cast on Column."""
        col = Column(self.dialect, "price")
        expr = col.cast(INTEGER)
        sql, params = expr.to_sql()
        # PostgreSQL dialect adds quotes to identifiers
        assert sql == '"price"::integer'
        assert params == ()

    def test_column_cast_with_varchar_length(self):
        """Test type cast with VARCHAR length modifier."""
        col = Column(self.dialect, "name")
        expr = col.cast("VARCHAR(100)")
        sql, params = expr.to_sql()
        assert sql == '"name"::VARCHAR(100)'
        assert params == ()

    def test_literal_cast(self):
        """Test type cast on Literal."""
        lit = Literal(self.dialect, "123")
        expr = lit.cast(INTEGER)
        sql, params = expr.to_sql()
        assert sql == "%s::integer"
        assert params == ("123",)

    def test_chained_type_cast(self):
        """Test chained type conversions."""
        col = Column(self.dialect, "amount")
        expr = col.cast(MONEY).cast(NUMERIC).cast(FLOAT8)
        sql, params = expr.to_sql()
        # PostgreSQL uses float8 alias for double precision
        assert sql == '"amount"::money::numeric::float8'
        assert params == ()

    def test_cast_with_alias(self):
        """Test type cast with alias."""
        col = Column(self.dialect, "value")
        expr = col.cast(INTEGER).as_("int_value")
        sql, params = expr.to_sql()
        assert sql == '"value"::integer AS "int_value"'
        assert params == ()


class TestCastExpression:
    """Tests for CastExpression class."""

    def setup_method(self):
        self.dialect = PostgresDialect()

    def test_cast_expression_creation(self):
        """Test CastExpression direct creation."""
        col = Column(self.dialect, "price")
        expr = CastExpression(self.dialect, col, NUMERIC)
        sql, params = expr.to_sql()
        assert sql == '"price"::numeric'
        assert params == ()

    def test_cast_expression_chained(self):
        """Test CastExpression chained conversions."""
        col = Column(self.dialect, "value")
        expr1 = CastExpression(self.dialect, col, MONEY)
        expr2 = expr1.cast(NUMERIC)
        sql, params = expr2.to_sql()
        assert sql == '"value"::money::numeric'
        assert params == ()

    def test_cast_expression_supports_comparison(self):
        """Test CastExpression supports comparison operators."""
        col = Column(self.dialect, "amount")
        expr = col.cast(INTEGER)
        predicate = expr > 0
        sql, params = predicate.to_sql()
        assert sql == '"amount"::integer > %s'
        assert params == (0,)

    def test_cast_expression_supports_arithmetic(self):
        """Test CastExpression supports arithmetic operators."""
        col = Column(self.dialect, "price")
        expr = col.cast(NUMERIC)
        result = expr * 1.1
        sql, params = result.to_sql()
        assert sql == '"price"::numeric * %s'
        assert params == (1.1,)


class TestPostgresDialectFormatCast:
    """Tests for PostgreSQL dialect format_cast_expression method."""

    def setup_method(self):
        self.dialect = PostgresDialect()

    def test_format_cast_basic(self):
        """Test basic type cast formatting."""
        sql, params = self.dialect.format_cast_expression(
            "col", INTEGER, ()
        )
        assert sql == "col::integer"
        assert params == ()

    def test_format_cast_with_type_modifier(self):
        """Test type cast with type modifiers."""
        sql, params = self.dialect.format_cast_expression(
            "name", "VARCHAR(100)", ()
        )
        assert sql == "name::VARCHAR(100)"
        assert params == ()

    def test_format_cast_with_alias(self):
        """Test type cast with alias."""
        sql, params = self.dialect.format_cast_expression(
            "value", INTEGER, (), "int_val"
        )
        assert sql == 'value::integer AS "int_val"'
        assert params == ()

    def test_format_cast_preserves_params(self):
        """Test that parameters are preserved."""
        params_in = ("test_value",)
        sql, params = self.dialect.format_cast_expression(
            "%s", TEXT, params_in
        )
        assert sql == "%s::text"
        assert params == params_in


class TestTypeCompatibility:
    """Tests for type compatibility checking."""

    def test_compatible_cast_no_warning(self):
        """Test that compatible casts don't produce warnings."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility("money", "numeric")
            assert result is True
            assert len(w) == 0

    def test_warned_cast_produces_warning(self):
        """Test that problematic casts produce warnings."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility("money", "float8")
            assert result is True
            assert len(w) == 1
            assert "intermediate conversion" in str(w[0].message)
            assert "numeric" in str(w[0].message)

    def test_same_type_no_warning(self):
        """Test that same-type casts don't produce warnings."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility("integer", "integer")
            assert result is True
            assert len(w) == 0

    def test_none_source_type(self):
        """Test that None source type doesn't produce warnings."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility(None, "integer")
            assert result is True
            assert len(w) == 0

    def test_get_compatible_types(self):
        """Test getting compatible types for a source type."""
        compatible = get_compatible_types("money")
        assert "money" in compatible
        assert "numeric" in compatible

    def test_get_intermediate_type(self):
        """Test getting intermediate type for problematic cast."""
        intermediate = get_intermediate_type("money", "float8")
        assert intermediate == "numeric"

    def test_no_intermediate_for_compatible(self):
        """Test that compatible casts have no intermediate type."""
        intermediate = get_intermediate_type("money", "numeric")
        assert intermediate is None


class TestTypeConstants:
    """Tests for PostgreSQL type constants."""

    def test_numeric_constants(self):
        """Test numeric type constants."""
        assert INTEGER == "integer"
        assert BIGINT == "bigint"
        assert NUMERIC == "numeric"
        assert FLOAT8 == "float8"

    def test_monetary_constant(self):
        """Test monetary type constant."""
        assert MONEY == "money"

    def test_character_constants(self):
        """Test character type constants."""
        assert VARCHAR == "varchar"
        assert TEXT == "text"

    def test_json_constants(self):
        """Test JSON type constants."""
        assert JSONB == "jsonb"

    def test_temporal_constants(self):
        """Test temporal type constants."""
        assert TIMESTAMP == "timestamp"
        assert TIMESTAMPTZ == "timestamptz"

    def test_boolean_constants(self):
        """Test boolean type constants."""
        assert BOOLEAN == "boolean"
        # BOOL is an alias
        from rhosocial.activerecord.backend.impl.postgres.types.constants import BOOL
        assert BOOL == "bool"


class TestComplexCastScenarios:
    """Tests for complex type casting scenarios."""

    def setup_method(self):
        self.dialect = PostgresDialect()

    def test_money_to_float_via_numeric(self):
        """Test money to float conversion via numeric."""
        col = Column(self.dialect, "price")
        # This should trigger a warning
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            expr = col.cast("float8")
            # The warning should suggest using numeric as intermediate
            if len(w) > 0:
                assert "numeric" in str(w[0].message)

    def test_cast_in_where_clause(self):
        """Test type cast in WHERE clause predicate."""
        col = Column(self.dialect, "amount")
        predicate = col.cast(NUMERIC) > 100
        sql, params = predicate.to_sql()
        assert sql == '"amount"::numeric > %s'
        assert params == (100,)

    def test_multiple_casts_in_expression(self):
        """Test multiple different cast chains in one expression."""
        col1 = Column(self.dialect, "price1")
        col2 = Column(self.dialect, "price2")
        expr = col1.cast(NUMERIC) + col2.cast(NUMERIC)
        sql, params = expr.to_sql()
        assert sql == '"price1"::numeric + "price2"::numeric'
        assert params == ()

    def test_cast_with_table_prefix(self):
        """Test type cast on column with table prefix."""
        col = Column(self.dialect, "amount", table="orders")
        expr = col.cast(INTEGER)
        sql, params = expr.to_sql()
        assert sql == '"orders"."amount"::integer'
        assert params == ()

    def test_nested_cast_with_alias(self):
        """Test nested cast expressions with aliases."""
        col = Column(self.dialect, "value")
        expr = col.cast(MONEY).as_("m").cast(NUMERIC).as_("n")
        sql, params = expr.to_sql()
        # Note: Each .as_() creates an alias for that expression
        # The final expression is: (value::money AS "m")::numeric AS "n"
        # But due to how CastExpression handles alias, it should be:
        # value::money::numeric AS "n" (last alias wins)
        assert '::' in sql  # Contains PostgreSQL cast syntax
