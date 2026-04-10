# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_postgres_array_functions.py
"""
Tests for PostgreSQL Array functions.

Functions: array_agg, array_append, array_cat, array_dims, array_fill,
           array_length, array_lower, array_ndims, array_position,
           array_positions, array_prepend, array_remove, array_replace,
           array_to_string, array_upper, unnest, string_to_array
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions.array import (
    array_agg,
    array_append,
    array_cat,
    array_dims,
    array_fill,
    array_length,
    array_lower,
    array_ndims,
    array_position,
    array_positions,
    array_prepend,
    array_remove,
    array_replace,
    array_to_string,
    array_upper,
    unnest,
    array_agg_distinct,
    string_to_array,
)


class TestPostgresArrayFunctions:
    """Tests for PostgreSQL array functions."""

    def test_array_agg(self, postgres_dialect: PostgresDialect):
        """Test array_agg() function."""
        result = array_agg(postgres_dialect, "value")
        assert result == "array_agg(value)"

    def test_array_append(self, postgres_dialect: PostgresDialect):
        """Test array_append() function."""
        result = array_append(postgres_dialect, "'{1,2}'", 3)
        assert result == "array_append('{1,2}', 3)"

    def test_array_cat(self, postgres_dialect: PostgresDialect):
        """Test array_cat() function."""
        result = array_cat(postgres_dialect, "'{1,2}'", "'{3,4}'")
        assert result == "array_cat('{1,2}', '{3,4}')"

    def test_array_dims(self, postgres_dialect: PostgresDialect):
        """Test array_dims() function."""
        result = array_dims(postgres_dialect, "'{{1,2},{3,4}}'")
        assert result == "array_dims('{{1,2},{3,4}}')"

    def test_array_fill(self, postgres_dialect: PostgresDialect):
        """Test array_fill() function."""
        result = array_fill(postgres_dialect, 0, "'{3,3}'")
        assert result == "array_fill(0, '{3,3}')"

    def test_array_length(self, postgres_dialect: PostgresDialect):
        """Test array_length() function."""
        result = array_length(postgres_dialect, "'{1,2,3}'", 1)
        assert result == "array_length('{1,2,3}', 1)"

    def test_array_lower(self, postgres_dialect: PostgresDialect):
        """Test array_lower() function."""
        result = array_lower(postgres_dialect, "'[1:3]={1,2,3}'", 1)
        assert result == "array_lower('[1:3]={1,2,3}', 1)"

    def test_array_ndims(self, postgres_dialect: PostgresDialect):
        """Test array_ndims() function."""
        result = array_ndims(postgres_dialect, "'{{1,2},{3,4}}'")
        assert result == "array_ndims('{{1,2},{3,4}}')"

    def test_array_position(self, postgres_dialect: PostgresDialect):
        """Test array_position() function."""
        result = array_position(postgres_dialect, "'{a,b,c}'", "'b'")
        assert result == "array_position('{a,b,c}', 'b')"

    def test_array_positions(self, postgres_dialect: PostgresDialect):
        """Test array_positions() function."""
        result = array_positions(postgres_dialect, "'[1,3,5,3]'", 3)
        assert result == "array_positions('[1,3,5,3]', 3)"

    def test_array_prepend(self, postgres_dialect: PostgresDialect):
        """Test array_prepend() function."""
        result = array_prepend(postgres_dialect, 1, "'{2,3}'")
        assert result == "array_prepend(1, '{2,3}')"

    def test_array_remove(self, postgres_dialect: PostgresDialect):
        """Test array_remove() function."""
        result = array_remove(postgres_dialect, "'{1,2,3,2}'", 2)
        assert result == "array_remove('{1,2,3,2}', 2)"

    def test_array_replace(self, postgres_dialect: PostgresDialect):
        """Test array_replace() function."""
        result = array_replace(postgres_dialect, "'{1,2,3}'", 2, 4)
        assert result == "array_replace('{1,2,3}', 2, 4)"

    def test_array_to_string(self, postgres_dialect: PostgresDialect):
        """Test array_to_string() function."""
        result = array_to_string(postgres_dialect, "'{1,2,3}'", ',')
        assert result == "array_to_string('{1,2,3}', ',')"

    def test_array_upper(self, postgres_dialect: PostgresDialect):
        """Test array_upper() function."""
        result = array_upper(postgres_dialect, "'[1:3]={1,2,3}'", 1)
        assert result == "array_upper('[1:3]={1,2,3}', 1)"

    def test_unnest_single(self, postgres_dialect: PostgresDialect):
        """Test unnest() with single array."""
        result = unnest(postgres_dialect, "'{1,2,3}'")
        assert result == "unnest('{1,2,3}')"

    def test_unnest_multiple(self, postgres_dialect: PostgresDialect):
        """Test unnest() with multiple arrays."""
        result = unnest(postgres_dialect, "'{1,2}'", "'{a,b}'")
        assert result == "unnest('{1,2}', '{a,b}')"

    def test_string_to_array(self, postgres_dialect: PostgresDialect):
        """Test string_to_array() function."""
        result = string_to_array(postgres_dialect, "'a,b,c'", ',')
        assert result == "string_to_array('a,b,c', ',')"

    def test_string_to_array_with_null(self, postgres_dialect: PostgresDialect):
        """Test string_to_array() function with null_string."""
        result = string_to_array(postgres_dialect, "'a,b,NULL,c'", ',', 'NULL')
        assert result == "string_to_array('a,b,NULL,c', ',', 'NULL')"