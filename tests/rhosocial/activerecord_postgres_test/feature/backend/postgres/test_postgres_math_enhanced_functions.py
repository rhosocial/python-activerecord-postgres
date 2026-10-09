# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_postgres_math_enhanced_functions.py
"""
Tests for PostgreSQL-specific enhanced math functions.

These include additional mathematical functions beyond the basic math module.
"""
from rhosocial.activerecord.backend.expression import Column, Literal
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions.math_enhanced import (
    round_,
    pow,
    power,
    sqrt,
    mod,
    ceil,
    floor,
    trunc,
    max_,
    min_,
    avg,
)


class TestPostgresMathEnhancedFunctions:
    """Tests for PostgreSQL enhanced math functions."""

    def test_round__default(self, postgres_dialect: PostgresDialect):
        """Test round_() with default precision."""
        result = round_(postgres_dialect, Column(postgres_dialect, "value"))
        sql, _ = result.to_sql()
        assert "ROUND(" in sql
        assert '"value"' in sql

    def test_round__with_precision(self, postgres_dialect: PostgresDialect):
        """Test round_() with precision."""
        result = round_(postgres_dialect, Column(postgres_dialect, "price"), Literal(postgres_dialect, 2))
        sql, _ = result.to_sql()
        assert "ROUND(" in sql

    def test_round__with_literal(self, postgres_dialect: PostgresDialect):
        """Test round_() with literal value."""
        result = round_(postgres_dialect, Literal(postgres_dialect, 3.14159), Literal(postgres_dialect, 2))
        sql, _ = result.to_sql()
        assert "ROUND(" in sql

    def test_pow(self, postgres_dialect: PostgresDialect):
        """Test pow() function."""
        result = pow(postgres_dialect, Column(postgres_dialect, "base"), Literal(postgres_dialect, 2))
        sql, _ = result.to_sql()
        assert "POW(" in sql

    def test_pow_both_columns(self, postgres_dialect: PostgresDialect):
        """Test pow() with both column references."""
        result = pow(
            postgres_dialect,
            Column(postgres_dialect, "x"),
            Column(postgres_dialect, "y")
        )
        sql, _ = result.to_sql()
        assert "POW(" in sql

    def test_power(self, postgres_dialect: PostgresDialect):
        """Test power() function (alias for POW)."""
        result = power(postgres_dialect, Literal(postgres_dialect, 2), Literal(postgres_dialect, 3))
        sql, _ = result.to_sql()
        assert "POWER(" in sql

    def test_sqrt(self, postgres_dialect: PostgresDialect):
        """Test sqrt() function."""
        result = sqrt(postgres_dialect, Column(postgres_dialect, "value"))
        sql, _ = result.to_sql()
        assert "SQRT(" in sql
        assert '"value"' in sql

    def test_sqrt_with_literal(self, postgres_dialect: PostgresDialect):
        """Test sqrt() with literal value."""
        result = sqrt(postgres_dialect, Literal(postgres_dialect, 16))
        sql, _ = result.to_sql()
        assert "SQRT(" in sql

    def test_mod(self, postgres_dialect: PostgresDialect):
        """Test mod() function."""
        result = mod(postgres_dialect, Column(postgres_dialect, "total"), Literal(postgres_dialect, 10))
        sql, _ = result.to_sql()
        assert "MOD(" in sql

    def test_mod_both_columns(self, postgres_dialect: PostgresDialect):
        """Test mod() with both column references."""
        result = mod(
            postgres_dialect,
            Column(postgres_dialect, "dividend"),
            Column(postgres_dialect, "divisor")
        )
        sql, _ = result.to_sql()
        assert "MOD(" in sql

    def test_ceil(self, postgres_dialect: PostgresDialect):
        """Test ceil() function."""
        result = ceil(postgres_dialect, Column(postgres_dialect, "value"))
        sql, _ = result.to_sql()
        assert "CEIL(" in sql
        assert '"value"' in sql

    def test_ceil_with_literal(self, postgres_dialect: PostgresDialect):
        """Test ceil() with literal value."""
        result = ceil(postgres_dialect, Literal(postgres_dialect, 3.14))
        sql, _ = result.to_sql()
        assert "CEIL(" in sql

    def test_floor(self, postgres_dialect: PostgresDialect):
        """Test floor() function."""
        result = floor(postgres_dialect, Column(postgres_dialect, "value"))
        sql, _ = result.to_sql()
        assert "FLOOR(" in sql
        assert '"value"' in sql

    def test_floor_with_literal(self, postgres_dialect: PostgresDialect):
        """Test floor() with literal value."""
        result = floor(postgres_dialect, Literal(postgres_dialect, 3.14))
        sql, _ = result.to_sql()
        assert "FLOOR(" in sql

    def test_trunc(self, postgres_dialect: PostgresDialect):
        """Test trunc() function."""
        result = trunc(postgres_dialect, Column(postgres_dialect, "value"))
        sql, _ = result.to_sql()
        assert "TRUNC(" in sql
        assert '"value"' in sql

    def test_trunc_with_literal(self, postgres_dialect: PostgresDialect):
        """Test trunc() with literal value."""
        result = trunc(postgres_dialect, Literal(postgres_dialect, 3.14))
        sql, _ = result.to_sql()
        assert "TRUNC(" in sql

    def test_trunc_with_precision(self, postgres_dialect: PostgresDialect):
        """Test trunc() with precision."""
        result = trunc(postgres_dialect, Literal(postgres_dialect, 3.14159), Literal(postgres_dialect, 2))
        sql, _ = result.to_sql()
        assert "TRUNC(" in sql

    def test_max__two_args(self, postgres_dialect: PostgresDialect):
        """Test max_() with two arguments (uses GREATEST)."""
        result = max_(postgres_dialect, Column(postgres_dialect, "a"), Column(postgres_dialect, "b"))
        sql, _ = result.to_sql()
        assert "GREATEST(" in sql

    def test_max__multiple_args(self, postgres_dialect: PostgresDialect):
        """Test max_() with multiple arguments (uses GREATEST)."""
        result = max_(
            postgres_dialect,
            Column(postgres_dialect, "a"),
            Column(postgres_dialect, "b"),
            Column(postgres_dialect, "c")
        )
        sql, _ = result.to_sql()
        assert "GREATEST(" in sql

    def test_max__with_literals(self, postgres_dialect: PostgresDialect):
        """Test max_() with literal values (uses GREATEST)."""
        result = max_(postgres_dialect, Literal(postgres_dialect, 1), Literal(postgres_dialect, 2), Literal(postgres_dialect, 3))
        sql, _ = result.to_sql()
        assert "GREATEST(" in sql

    def test_min__two_args(self, postgres_dialect: PostgresDialect):
        """Test min_() with two arguments (uses LEAST)."""
        result = min_(postgres_dialect, Column(postgres_dialect, "a"), Column(postgres_dialect, "b"))
        sql, _ = result.to_sql()
        assert "LEAST(" in sql

    def test_min__multiple_args(self, postgres_dialect: PostgresDialect):
        """Test min_() with multiple arguments (uses LEAST)."""
        result = min_(
            postgres_dialect,
            Column(postgres_dialect, "a"),
            Column(postgres_dialect, "b"),
            Column(postgres_dialect, "c")
        )
        sql, _ = result.to_sql()
        assert "LEAST(" in sql

    def test_min__with_literals(self, postgres_dialect: PostgresDialect):
        """Test min_() with literal values (uses LEAST)."""
        result = min_(postgres_dialect, Literal(postgres_dialect, 1), Literal(postgres_dialect, 2), Literal(postgres_dialect, 3))
        sql, _ = result.to_sql()
        assert "LEAST(" in sql

    def test_avg(self, postgres_dialect: PostgresDialect):
        """Test avg() aggregate function."""
        result = avg(postgres_dialect, Column(postgres_dialect, "price"))
        sql, _ = result.to_sql()
        assert "AVG(" in sql
        assert '"price"' in sql

    def test_avg_with_literal(self, postgres_dialect: PostgresDialect):
        """Test avg() with literal value."""
        result = avg(postgres_dialect, Literal(postgres_dialect, 100))
        sql, _ = result.to_sql()
        assert "AVG(" in sql

    def test_round__with_string_integer(self, postgres_dialect: PostgresDialect):
        """Test round_() with string integer value."""
        result = round_(postgres_dialect, Literal(postgres_dialect, "123"), Literal(postgres_dialect, 2))
        sql, _ = result.to_sql()
        assert "ROUND(" in sql

    def test_round__with_string_float(self, postgres_dialect: PostgresDialect):
        """Test round_() with string float value."""
        result = round_(postgres_dialect, Literal(postgres_dialect, "3.14159"), Literal(postgres_dialect, 2))
        sql, _ = result.to_sql()
        assert "ROUND(" in sql

    def test_round__of_a_column(self, postgres_dialect: PostgresDialect):
        """A column is named by the caller, not inferred from a string."""
        result = round_(postgres_dialect, Column(postgres_dialect, "column_name"), 2)
        sql, _ = result.to_sql()
        assert "ROUND(" in sql
        assert '"column_name"' in sql

    def test_round__of_string_text_is_data(self, postgres_dialect: PostgresDialect):
        """A string is a value. It used to be parsed as a number when it looked
        like one and read as a column name when it did not, so the same call
        meant two different things depending on the argument's spelling."""
        result = round_(postgres_dialect, Literal(postgres_dialect, "3.5"), 2)
        sql, params = result.to_sql()
        assert "ROUND(" in sql
        assert params == ("3.5", 2)

    def test_pow_with_string_integer(self, postgres_dialect: PostgresDialect):
        """Test pow() with string integer exponent."""
        result = pow(postgres_dialect, Column(postgres_dialect, "base"), Literal(postgres_dialect, "2"))
        sql, _ = result.to_sql()
        assert "POW(" in sql

    def test_sqrt_with_string_integer(self, postgres_dialect: PostgresDialect):
        """Test sqrt() with string integer value."""
        result = sqrt(postgres_dialect, Literal(postgres_dialect, "16"))
        sql, _ = result.to_sql()
        assert "SQRT(" in sql

    def test_mod_with_string_divisor(self, postgres_dialect: PostgresDialect):
        """Test mod() with string divisor."""
        result = mod(postgres_dialect, Column(postgres_dialect, "total"), Literal(postgres_dialect, "10"))
        sql, _ = result.to_sql()
        assert "MOD(" in sql

    def test_max__with_string_values(self, postgres_dialect: PostgresDialect):
        """String arguments are values, so they are bound rather than quoted
        as identifiers."""
        result = max_(postgres_dialect,
                      Literal(postgres_dialect, "a"),
                      Literal(postgres_dialect, "b"))
        sql, params = result.to_sql()
        assert "GREATEST(" in sql
        assert params == ("a", "b")

    def test_min__with_string_values(self, postgres_dialect: PostgresDialect):
        """String arguments are values, so they are bound rather than quoted
        as identifiers."""
        result = min_(postgres_dialect,
                      Literal(postgres_dialect, "a"),
                      Literal(postgres_dialect, "b"))
        sql, params = result.to_sql()
        assert "LEAST(" in sql
        assert params == ("a", "b")

    def test_avg_with_string_literal(self, postgres_dialect: PostgresDialect):
        """Test avg() with string numeric value."""
        result = avg(postgres_dialect, Literal(postgres_dialect, "100"))
        sql, _ = result.to_sql()
        assert "AVG(" in sql