# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_range_functions.py
"""Unit tests for PostgreSQL range functions.

Tests for:
- Range operators (@>, <@, &&, -|-, <<, >>, &<, &>)
- Range operations (+, *, -)
- Range functions (lower, upper, isempty, lower_inc, upper_inc, lower_inf, upper_inf)
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import operators, core
from rhosocial.activerecord.backend.impl.postgres.functions.range import (
    range_contains,
    range_contained_by,
    range_contains_range,
    range_overlaps,
    range_adjacent,
    range_strictly_left_of,
    range_strictly_right_of,
    range_not_extend_right,
    range_not_extend_left,
    range_union,
    range_intersection,
    range_difference,
    range_lower,
    range_upper,
    range_is_empty,
    range_lower_inc,
    range_upper_inc,
    range_lower_inf,
    range_upper_inf,
)
from rhosocial.activerecord.backend.impl.postgres.types.range import PostgresRange


class TestRangeContains:
    """Test range_contains function."""

    def test_range_contains_element(self):
        """Test range contains element."""
        dialect = PostgresDialect((14, 0, 0))
        result = range_contains(dialect, "int4range_col", 5)
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "@>" in sql
        assert params == ("int4range_col", 5)

    def test_range_contains_range_object(self):
        """Test range contains with PostgresRange object."""
        dialect = PostgresDialect((14, 0, 0))
        r = PostgresRange(1, 10)
        result = range_contains(dialect, r, 5)
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "@>" in sql
        assert "[1,10)" in params

    def test_range_contains_both_ranges(self):
        """Test range contains with both as range objects."""
        dialect = PostgresDialect((14, 0, 0))
        r1 = PostgresRange(1, 100)
        r2 = PostgresRange(10, 20)
        result = range_contains(dialect, r1, r2)
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "@>" in sql
        assert "[1,100)" in params
        assert "[10,20)" in params


class TestRangeContainedBy:
    """Test range_contained_by function."""

    def test_element_contained_by_range(self):
        """Test element contained by range."""
        dialect = PostgresDialect((14, 0, 0))
        result = range_contained_by(dialect, 5, "int4range_col")
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "<@" in sql
        assert params == (5, "int4range_col")

    def test_range_contained_by_range_object(self):
        """Test range contained by with PostgresRange object."""
        dialect = PostgresDialect((14, 0, 0))
        r = PostgresRange(1, 10)
        result = range_contained_by(dialect, 5, r)
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "<@" in sql
        assert "[1,10)" in params


class TestRangeContainsRange:
    """Test range_contains_range function."""

    def test_range_contains_range_columns(self):
        """Test range contains range with columns."""
        dialect = PostgresDialect((14, 0, 0))
        result = range_contains_range(dialect, "col1", "col2")
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "@>" in sql
        assert params == ("col1", "col2")

    def test_range_contains_range_objects(self):
        """Test range contains range with objects."""
        dialect = PostgresDialect((14, 0, 0))
        r1 = PostgresRange(1, 100)
        r2 = PostgresRange(10, 20)
        result = range_contains_range(dialect, r1, r2)
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "@>" in sql
        assert "[1,100)" in params
        assert "[10,20)" in params


class TestRangeOverlaps:
    """Test range_overlaps function."""

    def test_range_overlaps_columns(self):
        """Test range overlaps with columns."""
        dialect = PostgresDialect((14, 0, 0))
        result = range_overlaps(dialect, "col1", "col2")
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "&&" in sql
        assert params == ("col1", "col2")

    def test_range_overlaps_objects(self):
        """Test range overlaps with objects."""
        dialect = PostgresDialect((14, 0, 0))
        r1 = PostgresRange(1, 10)
        r2 = PostgresRange(5, 15)
        result = range_overlaps(dialect, r1, r2)
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "&&" in sql
        assert "[1,10)" in params
        assert "[5,15)" in params


class TestRangeAdjacent:
    """Test range_adjacent function."""

    def test_range_adjacent_columns(self):
        """Test range adjacent with columns."""
        dialect = PostgresDialect((14, 0, 0))
        result = range_adjacent(dialect, "col1", "col2")
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "-|-" in sql
        assert params == ("col1", "col2")

    def test_range_adjacent_objects(self):
        """Test range adjacent with objects."""
        dialect = PostgresDialect((14, 0, 0))
        r1 = PostgresRange(1, 10)
        r2 = PostgresRange(10, 20)
        result = range_adjacent(dialect, r1, r2)
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "-|-" in sql
        assert "[1,10)" in params
        assert "[10,20)" in params


class TestRangeStrictlyLeftOf:
    """Test range_strictly_left_of function."""

    def test_range_strictly_left_columns(self):
        """Test range strictly left of with columns."""
        dialect = PostgresDialect((14, 0, 0))
        result = range_strictly_left_of(dialect, "col1", "col2")
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "<<" in sql
        assert params == ("col1", "col2")

    def test_range_strictly_left_objects(self):
        """Test range strictly left of with objects."""
        dialect = PostgresDialect((14, 0, 0))
        r1 = PostgresRange(1, 10)
        r2 = PostgresRange(20, 30)
        result = range_strictly_left_of(dialect, r1, r2)
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "<<" in sql
        assert "[1,10)" in params
        assert "[20,30)" in params


class TestRangeStrictlyRightOf:
    """Test range_strictly_right_of function."""

    def test_range_strictly_right_columns(self):
        """Test range strictly right of with columns."""
        dialect = PostgresDialect((14, 0, 0))
        result = range_strictly_right_of(dialect, "col1", "col2")
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert ">>" in sql
        assert params == ("col1", "col2")

    def test_range_strictly_right_objects(self):
        """Test range strictly right of with objects."""
        dialect = PostgresDialect((14, 0, 0))
        r1 = PostgresRange(20, 30)
        r2 = PostgresRange(1, 10)
        result = range_strictly_right_of(dialect, r1, r2)
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert ">>" in sql
        assert "[20,30)" in params
        assert "[1,10)" in params


class TestRangeNotExtendRight:
    """Test range_not_extend_right function."""

    def test_range_not_extend_right_columns(self):
        """Test range not extend right with columns."""
        dialect = PostgresDialect((14, 0, 0))
        result = range_not_extend_right(dialect, "col1", "col2")
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "&<" in sql
        assert params == ("col1", "col2")

    def test_range_not_extend_right_objects(self):
        """Test range not extend right with objects."""
        dialect = PostgresDialect((14, 0, 0))
        r1 = PostgresRange(1, 10)
        r2 = PostgresRange(5, 20)
        result = range_not_extend_right(dialect, r1, r2)
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "&<" in sql
        assert "[1,10)" in params
        assert "[5,20)" in params


class TestRangeNotExtendLeft:
    """Test range_not_extend_left function."""

    def test_range_not_extend_left_columns(self):
        """Test range not extend left with columns."""
        dialect = PostgresDialect((14, 0, 0))
        result = range_not_extend_left(dialect, "col1", "col2")
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "&>" in sql
        assert params == ("col1", "col2")

    def test_range_not_extend_left_objects(self):
        """Test range not extend left with objects."""
        dialect = PostgresDialect((14, 0, 0))
        r1 = PostgresRange(10, 20)
        r2 = PostgresRange(1, 15)
        result = range_not_extend_left(dialect, r1, r2)
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "&>" in sql
        assert "[10,20)" in params
        assert "[1,15)" in params


class TestRangeUnion:
    """Test range_union function."""

    def test_range_union_columns(self):
        """Test range union with columns."""
        dialect = PostgresDialect((14, 0, 0))
        result = range_union(dialect, "col1", "col2")
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "+" in sql
        assert params == ("col1", "col2")

    def test_range_union_objects(self):
        """Test range union with objects."""
        dialect = PostgresDialect((14, 0, 0))
        r1 = PostgresRange(1, 10)
        r2 = PostgresRange(10, 20)
        result = range_union(dialect, r1, r2)
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "+" in sql
        assert "[1,10)" in params
        assert "[10,20)" in params


class TestRangeIntersection:
    """Test range_intersection function."""

    def test_range_intersection_columns(self):
        """Test range intersection with columns."""
        dialect = PostgresDialect((14, 0, 0))
        result = range_intersection(dialect, "col1", "col2")
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "*" in sql
        assert params == ("col1", "col2")

    def test_range_intersection_objects(self):
        """Test range intersection with objects."""
        dialect = PostgresDialect((14, 0, 0))
        r1 = PostgresRange(1, 20)
        r2 = PostgresRange(10, 30)
        result = range_intersection(dialect, r1, r2)
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "*" in sql
        assert "[1,20)" in params
        assert "[10,30)" in params


class TestRangeDifference:
    """Test range_difference function."""

    def test_range_difference_columns(self):
        """Test range difference with columns."""
        dialect = PostgresDialect((14, 0, 0))
        result = range_difference(dialect, "col1", "col2")
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "-" in sql
        assert params == ("col1", "col2")

    def test_range_difference_objects(self):
        """Test range difference with objects."""
        dialect = PostgresDialect((14, 0, 0))
        r1 = PostgresRange(1, 30)
        r2 = PostgresRange(10, 20)
        result = range_difference(dialect, r1, r2)
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "-" in sql
        assert "[1,30)" in params
        assert "[10,20)" in params


class TestRangeFunctions:
    """Test range functions (lower, upper, isempty, etc.)."""

    def test_range_lower_column(self):
        """Test range lower function with column."""
        dialect = PostgresDialect((14, 0, 0))
        result = range_lower(dialect, "int4range_col")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "lower" in sql.lower()
        assert params == ("int4range_col",)

    def test_range_lower_object(self):
        """Test range lower function with object."""
        dialect = PostgresDialect((14, 0, 0))
        r = PostgresRange(1, 10)
        result = range_lower(dialect, r)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "lower" in sql.lower()
        assert "[1,10)" in params

    def test_range_upper_column(self):
        """Test range upper function with column."""
        dialect = PostgresDialect((14, 0, 0))
        result = range_upper(dialect, "int4range_col")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "upper" in sql.lower()
        assert params == ("int4range_col",)

    def test_range_upper_object(self):
        """Test range upper function with object."""
        dialect = PostgresDialect((14, 0, 0))
        r = PostgresRange(1, 10)
        result = range_upper(dialect, r)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "upper" in sql.lower()
        assert "[1,10)" in params

    def test_range_is_empty_column(self):
        """Test range isempty function with column."""
        dialect = PostgresDialect((14, 0, 0))
        result = range_is_empty(dialect, "int4range_col")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "isempty" in sql.lower()
        assert params == ("int4range_col",)

    def test_range_is_empty_object(self):
        """Test range isempty function with object."""
        dialect = PostgresDialect((14, 0, 0))
        r = PostgresRange.empty()
        result = range_is_empty(dialect, r)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "isempty" in sql.lower()
        assert "empty" in params

    def test_range_lower_inc_column(self):
        """Test range lower_inc function with column."""
        dialect = PostgresDialect((14, 0, 0))
        result = range_lower_inc(dialect, "int4range_col")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "lower_inc" in sql.lower()
        assert params == ("int4range_col",)

    def test_range_lower_inc_object(self):
        """Test range lower_inc function with object."""
        dialect = PostgresDialect((14, 0, 0))
        r = PostgresRange(1, 10)
        result = range_lower_inc(dialect, r)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "lower_inc" in sql.lower()
        assert "[1,10)" in params

    def test_range_upper_inc_column(self):
        """Test range upper_inc function with column."""
        dialect = PostgresDialect((14, 0, 0))
        result = range_upper_inc(dialect, "int4range_col")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "upper_inc" in sql.lower()
        assert params == ("int4range_col",)

    def test_range_upper_inc_object(self):
        """Test range upper_inc function with object."""
        dialect = PostgresDialect((14, 0, 0))
        r = PostgresRange(1, 10, upper_inc=True)
        result = range_upper_inc(dialect, r)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "upper_inc" in sql.lower()
        assert "[1,10]" in params

    def test_range_lower_inf_column(self):
        """Test range lower_inf function with column."""
        dialect = PostgresDialect((14, 0, 0))
        result = range_lower_inf(dialect, "int4range_col")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "lower_inf" in sql.lower()
        assert params == ("int4range_col",)

    def test_range_lower_inf_unbounded(self):
        """Test range lower_inf function with unbounded range."""
        dialect = PostgresDialect((14, 0, 0))
        r = PostgresRange(None, 10)
        result = range_lower_inf(dialect, r)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "lower_inf" in sql.lower()

    def test_range_upper_inf_column(self):
        """Test range upper_inf function with column."""
        dialect = PostgresDialect((14, 0, 0))
        result = range_upper_inf(dialect, "int4range_col")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "upper_inf" in sql.lower()
        assert params == ("int4range_col",)

    def test_range_upper_inf_unbounded(self):
        """Test range upper_inf function with unbounded range."""
        dialect = PostgresDialect((14, 0, 0))
        r = PostgresRange(1, None)
        result = range_upper_inf(dialect, r)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "upper_inf" in sql.lower()
