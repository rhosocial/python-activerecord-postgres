# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_cube_types.py
"""Unit tests for PostgreSQL cube type.

Tests for:
- PostgresCube data class
- PostgresCubeAdapter conversion
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.types.cube import PostgresCube
from rhosocial.activerecord.backend.impl.postgres.adapters.cube import PostgresCubeAdapter


class TestPostgresCube:
    """Tests for PostgresCube data class."""

    def test_create_point(self):
        c = PostgresCube([1.0, 2.0, 3.0])
        assert c.lower == [1.0, 2.0, 3.0]
        assert c.upper is None
        assert c.is_point
        assert c.dimensions == 3

    def test_create_box(self):
        c = PostgresCube([1.0, 1.0], [3.0, 4.0])
        assert c.lower == [1.0, 1.0]
        assert c.upper == [3.0, 4.0]
        assert not c.is_point
        assert c.dimensions == 2

    def test_empty_lower_raises_error(self):
        with pytest.raises(ValueError, match="at least one dimension"):
            PostgresCube([])

    def test_dimension_mismatch_raises_error(self):
        with pytest.raises(ValueError, match="dimensions"):
            PostgresCube([1.0, 2.0], [3.0, 4.0, 5.0])

    def test_to_postgres_string_point(self):
        c = PostgresCube([1.0, 2.0, 3.0])
        assert c.to_postgres_string() == "(1.0, 2.0, 3.0)"

    def test_to_postgres_string_box(self):
        c = PostgresCube([1.0, 1.0], [3.0, 4.0])
        assert c.to_postgres_string() == "(1.0, 1.0),(3.0, 4.0)"

    def test_from_postgres_string_point(self):
        c = PostgresCube.from_postgres_string("(1, 2, 3)")
        assert c.lower == [1.0, 2.0, 3.0]
        assert c.upper is None

    def test_from_postgres_string_box(self):
        c = PostgresCube.from_postgres_string("(1,2),(3,4)")
        assert c.lower == [1.0, 2.0]
        assert c.upper == [3.0, 4.0]

    def test_from_postgres_string_single_dim(self):
        c = PostgresCube.from_postgres_string("(5)")
        assert c.lower == [5.0]
        assert c.upper is None

    def test_from_postgres_string_invalid_format(self):
        with pytest.raises(ValueError, match="Invalid cube format"):
            PostgresCube.from_postgres_string("no parentheses")

    def test_to_sql_literal_point(self):
        c = PostgresCube([1.0, 2.0])
        assert c.to_sql_literal() == "'(1.0, 2.0)'::cube"

    def test_to_sql_literal_box(self):
        c = PostgresCube([1.0, 1.0], [3.0, 4.0])
        assert c.to_sql_literal() == "'(1.0, 1.0),(3.0, 4.0)'::cube"

    def test_equality(self):
        a = PostgresCube([1.0, 2.0])
        b = PostgresCube([1.0, 2.0])
        assert a == b

    def test_equality_with_string(self):
        c = PostgresCube([1.0, 2.0])
        assert c == "(1.0, 2.0)"

    def test_inequality(self):
        a = PostgresCube([1.0, 2.0])
        b = PostgresCube([3.0, 4.0])
        assert a != b

    def test_hash(self):
        a = PostgresCube([1.0, 2.0])
        b = PostgresCube([1.0, 2.0])
        assert hash(a) == hash(b)
        assert len({a, b}) == 1

    def test_str_point(self):
        c = PostgresCube([1.0, 2.0])
        assert str(c) == "(1.0, 2.0)"

    def test_str_box(self):
        c = PostgresCube([1.0, 1.0], [3.0, 4.0])
        assert str(c) == "(1.0, 1.0),(3.0, 4.0)"

    def test_repr(self):
        c = PostgresCube([1.0, 2.0])
        r = repr(c)
        assert "PostgresCube" in r

    def test_frozen(self):
        c = PostgresCube([1.0, 2.0])
        with pytest.raises(AttributeError):
            c.lower = [3.0, 4.0]

    def test_roundtrip_point(self):
        original = PostgresCube([1.5, 2.5, 3.5])
        s = original.to_postgres_string()
        restored = PostgresCube.from_postgres_string(s)
        assert restored == original

    def test_roundtrip_box(self):
        original = PostgresCube([1.0, 1.0], [3.0, 4.0])
        s = original.to_postgres_string()
        restored = PostgresCube.from_postgres_string(s)
        assert restored == original


class TestPostgresCubeAdapter:
    """Tests for PostgresCubeAdapter."""

    def test_supported_types(self):
        adapter = PostgresCubeAdapter()
        supported = adapter.supported_types
        assert PostgresCube in supported

    def test_to_database_cube(self):
        adapter = PostgresCubeAdapter()
        c = PostgresCube([1.0, 2.0])
        result = adapter.to_database(c, str)
        assert result == "(1.0, 2.0)"

    def test_to_database_string(self):
        adapter = PostgresCubeAdapter()
        result = adapter.to_database("(1,2)", str)
        assert result == "(1,2)"

    def test_to_database_none(self):
        adapter = PostgresCubeAdapter()
        result = adapter.to_database(None, str)
        assert result is None

    def test_to_database_invalid_type(self):
        adapter = PostgresCubeAdapter()
        with pytest.raises(TypeError, match="Cannot convert"):
            adapter.to_database(12345, str)

    def test_from_database_string(self):
        adapter = PostgresCubeAdapter()
        result = adapter.from_database("(1,2,3)", PostgresCube)
        assert isinstance(result, PostgresCube)
        assert result.lower == [1.0, 2.0, 3.0]

    def test_from_database_cube(self):
        adapter = PostgresCubeAdapter()
        c = PostgresCube([1.0, 2.0])
        result = adapter.from_database(c, PostgresCube)
        assert result is c

    def test_from_database_none(self):
        adapter = PostgresCubeAdapter()
        result = adapter.from_database(None, PostgresCube)
        assert result is None

    def test_from_database_invalid_type(self):
        adapter = PostgresCubeAdapter()
        with pytest.raises(TypeError, match="Cannot convert"):
            adapter.from_database(12345, PostgresCube)

    def test_batch_to_database(self):
        adapter = PostgresCubeAdapter()
        values = [
            PostgresCube([1.0, 2.0]),
            "(3,4)",
            None,
        ]
        results = adapter.to_database_batch(values, str)
        assert results[0] == "(1.0, 2.0)"
        assert results[1] == "(3,4)"
        assert results[2] is None

    def test_batch_from_database(self):
        adapter = PostgresCubeAdapter()
        values = ["(1,2)", "(3,4)", None]
        results = adapter.from_database_batch(values, PostgresCube)
        assert isinstance(results[0], PostgresCube)
        assert results[0].lower == [1.0, 2.0]
        assert isinstance(results[1], PostgresCube)
        assert results[2] is None