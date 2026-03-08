# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_jsonpath_types.py
"""
Unit tests for PostgreSQL jsonpath type support.

Tests for:
- PostgresJsonPath data class
- PostgresJsonPathAdapter conversion
- Path builder utility functions
- SQL expression generators
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.types.json import PostgresJsonPath
from rhosocial.activerecord.backend.impl.postgres.adapters.json import PostgresJsonPathAdapter
from rhosocial.activerecord.backend.impl.postgres.functions.json import (
    json_path_root,
    json_path_key,
    json_path_index,
    json_path_wildcard,
    json_path_filter,
    jsonb_path_query,
    jsonb_path_query_first,
    jsonb_path_exists,
    jsonb_path_match,
)


class TestPostgresJsonPath:
    """Tests for PostgresJsonPath data class."""

    def test_create_root_path(self):
        """Test creating root path."""
        path = PostgresJsonPath('$')
        assert path.path == '$'

    def test_create_nested_path(self):
        """Test creating nested path."""
        path = PostgresJsonPath('$.store.book')
        assert path.path == '$.store.book'

    def test_create_array_index_path(self):
        """Test creating array index path."""
        path = PostgresJsonPath('$[0]')
        assert path.path == '$[0]'

    def test_create_complex_path(self):
        """Test creating complex path with array and nested keys."""
        path = PostgresJsonPath('$.items[*].name')
        assert path.path == '$.items[*].name'

    def test_create_path_with_filter(self):
        """Test creating path with filter expression."""
        path = PostgresJsonPath('$.items[*]?(@.price < 10)')
        assert path.path == '$.items[*]?(@.price < 10)'

    def test_empty_path_raises_error(self):
        """Test that empty path raises ValueError."""
        with pytest.raises(ValueError, match="jsonpath cannot be empty"):
            PostgresJsonPath('')

    def test_path_without_dollar_raises_error(self):
        """Test that path without $ prefix raises ValueError."""
        with pytest.raises(ValueError, match="jsonpath must start with"):
            PostgresJsonPath('.store.book')

    def test_path_string_representation(self):
        """Test string representation of path."""
        path = PostgresJsonPath('$.store.book')
        assert str(path) == '$.store.book'

    def test_path_repr(self):
        """Test repr of path."""
        path = PostgresJsonPath('$.store')
        assert repr(path) == "PostgresJsonPath('$.store')"

    def test_path_equality_with_path(self):
        """Test equality between two PostgresJsonPath objects."""
        path1 = PostgresJsonPath('$.store')
        path2 = PostgresJsonPath('$.store')
        path3 = PostgresJsonPath('$.book')
        assert path1 == path2
        assert path1 != path3

    def test_path_equality_with_string(self):
        """Test equality with string."""
        path = PostgresJsonPath('$.store')
        assert path == '$.store'
        assert path != '$.book'

    def test_path_hash(self):
        """Test that paths are hashable."""
        path1 = PostgresJsonPath('$.store')
        path2 = PostgresJsonPath('$.store')
        assert hash(path1) == hash(path2)
        assert len({path1, path2}) == 1


class TestPostgresJsonPathValidation:
    """Tests for PostgresJsonPath validation."""

    def test_valid_root_path(self):
        """Test that root path is valid."""
        path = PostgresJsonPath('$')
        assert path.is_valid() is True

    def test_valid_nested_path(self):
        """Test that nested path is valid."""
        path = PostgresJsonPath('$.store.book.author')
        assert path.is_valid() is True

    def test_valid_array_path(self):
        """Test that array path is valid."""
        path = PostgresJsonPath('$.items[0].name')
        assert path.is_valid() is True

    def test_valid_wildcard_path(self):
        """Test that wildcard path is valid."""
        path = PostgresJsonPath('$.items[*]')
        assert path.is_valid() is True

    def test_valid_filter_path(self):
        """Test that filter path is valid."""
        path = PostgresJsonPath('$.items[*]?(@.price < 10)')
        assert path.is_valid() is True

    def test_valid_complex_filter_path(self):
        """Test that complex filter path is valid."""
        path = PostgresJsonPath('$.items[*]?(@.price > 5 && @.price < 10)')
        assert path.is_valid() is True

    def test_invalid_unmatched_bracket(self):
        """Test that unmatched bracket makes path invalid."""
        path = PostgresJsonPath('$[0')
        assert path.is_valid() is False

    def test_invalid_unmatched_paren(self):
        """Test that unmatched parenthesis makes path invalid."""
        path = PostgresJsonPath('$.items[0]?(@.price < 10')
        assert path.is_valid() is False


class TestPostgresJsonPathBuilder:
    """Tests for PostgresJsonPath builder methods."""

    def test_key_simple(self):
        """Test adding simple key."""
        path = PostgresJsonPath('$').key('name')
        assert path.path == '$.name'

    def test_key_nested(self):
        """Test adding nested keys."""
        path = PostgresJsonPath('$').key('store').key('book')
        assert path.path == '$.store.book'

    def test_key_with_special_chars(self):
        """Test key with special characters is quoted."""
        path = PostgresJsonPath('$').key('book-name')
        assert path.path == '$."book-name"'

    def test_key_with_space(self):
        """Test key with space is quoted."""
        path = PostgresJsonPath('$').key('book name')
        assert path.path == '$."book name"'

    def test_key_starting_with_digit(self):
        """Test key starting with digit is quoted."""
        path = PostgresJsonPath('$').key('1st')
        assert path.path == '$."1st"'

    def test_index_integer(self):
        """Test adding integer index."""
        path = PostgresJsonPath('$').index(0)
        assert path.path == '$[0]'

    def test_index_negative(self):
        """Test adding negative index."""
        path = PostgresJsonPath('$').index(-1)
        assert path.path == '$[-1]'

    def test_index_last(self):
        """Test adding 'last' index."""
        path = PostgresJsonPath('$').index('last')
        assert path.path == '$[last]'

    def test_index_last_offset(self):
        """Test adding 'last-N' index."""
        path = PostgresJsonPath('$').index('last-1')
        assert path.path == '$[last-1]'

    def test_index_chained(self):
        """Test chained index access."""
        path = PostgresJsonPath('$').index(0).key('name')
        assert path.path == '$[0].name'

    def test_wildcard_array(self):
        """Test array wildcard."""
        path = PostgresJsonPath('$').wildcard_array()
        assert path.path == '$[*]'

    def test_wildcard_object(self):
        """Test object wildcard."""
        path = PostgresJsonPath('$').wildcard_object()
        assert path.path == '$.*'

    def test_wildcard_chained(self):
        """Test chained wildcards."""
        path = PostgresJsonPath('$').key('items').wildcard_array().key('name')
        assert path.path == '$.items[*].name'

    def test_filter_simple(self):
        """Test simple filter."""
        path = PostgresJsonPath('$').wildcard_array().filter('@.price < 10')
        assert path.path == '$[*]?(@.price < 10)'

    def test_filter_complex(self):
        """Test complex filter."""
        path = PostgresJsonPath('$').key('items').wildcard_array().filter('@.price > 5 && @.price < 10')
        assert path.path == '$.items[*]?(@.price > 5 && @.price < 10)'

    def test_combined_operations(self):
        """Test combining multiple operations."""
        path = PostgresJsonPath('$').key('store').key('books').wildcard_array().filter('@.price < 20').key('title')
        assert path.path == '$.store.books[*]?(@.price < 20).title'


class TestPostgresJsonPathToSqlString:
    """Tests for to_sql_string method."""

    def test_simple_path(self):
        """Test simple path to SQL string."""
        path = PostgresJsonPath('$.name')
        assert path.to_sql_string() == "'$.name'"

    def test_path_with_quotes(self):
        """Test path with single quotes."""
        path = PostgresJsonPath("$.items['name']")
        # Single quotes in the path are escaped for SQL
        result = path.to_sql_string()
        assert "'" in result  # Contains quote escaping
        assert ".items" in result

    def test_complex_path(self):
        """Test complex path to SQL string."""
        path = PostgresJsonPath('$.items[*]?(@.price < 10)')
        assert path.to_sql_string() == "'$.items[*]?(@.price < 10)'"


class TestPathBuilderFunctions:
    """Tests for path builder utility functions."""

    def test_json_path_root(self):
        """Test creating root path."""
        path = json_path_root()
        assert path.path == '$'
        assert isinstance(path, PostgresJsonPath)

    def test_json_path_key_from_string(self):
        """Test adding key to string path."""
        path = json_path_key('$', 'name')
        assert path.path == '$.name'

    def test_json_path_key_from_path(self):
        """Test adding key to PostgresJsonPath."""
        path = json_path_key(json_path_root(), 'store')
        assert path.path == '$.store'

    def test_json_path_key_chained(self):
        """Test chained key access."""
        path = json_path_key(json_path_key('$', 'store'), 'book')
        assert path.path == '$.store.book'

    def test_json_path_index_from_string(self):
        """Test adding index to string path."""
        path = json_path_index('$', 0)
        assert path.path == '$[0]'

    def test_json_path_index_from_path(self):
        """Test adding index to PostgresJsonPath."""
        path = json_path_index(json_path_root(), 0)
        assert path.path == '$[0]'

    def test_json_path_index_last(self):
        """Test using 'last' index."""
        path = json_path_index('$.items', 'last')
        assert path.path == '$.items[last]'

    def test_json_path_wildcard_array(self):
        """Test array wildcard."""
        path = json_path_wildcard('$')
        assert path.path == '$[*]'

    def test_json_path_wildcard_object(self):
        """Test object wildcard."""
        path = json_path_wildcard('$', array=False)
        assert path.path == '$.*'

    def test_json_path_wildcard_chained(self):
        """Test chained wildcard."""
        path = json_path_wildcard(json_path_key('$', 'items'))
        assert path.path == '$.items[*]'

    def test_json_path_filter_from_string(self):
        """Test adding filter to string path."""
        path = json_path_filter('$.items[*]', '@.price < 10')
        assert path.path == '$.items[*]?(@.price < 10)'

    def test_json_path_filter_from_path(self):
        """Test adding filter to PostgresJsonPath."""
        path = json_path_filter(json_path_wildcard(json_path_key('$', 'items')), '@.active')
        assert path.path == '$.items[*]?(@.active)'

    def test_complex_path_building(self):
        """Test building complex path using utilities."""
        path = json_path_filter(
            json_path_wildcard(
                json_path_key(
                    json_path_key('$', 'store'),
                    'books'
                )
            ),
            '@.price < 20'
        )
        assert path.path == '$.store.books[*]?(@.price < 20)'


class TestSqlExpressionGenerators:
    """Tests for SQL expression generator functions."""

    def test_jsonb_path_query_simple(self):
        """Test simple jsonb_path_query."""
        expr = jsonb_path_query(None, 'data', '$.items[*]')
        assert expr == "jsonb_path_query(data, '$.items[*]')"

    def test_jsonb_path_query_with_path_object(self):
        """Test jsonb_path_query with PostgresJsonPath."""
        path = PostgresJsonPath('$.items[*]')
        expr = jsonb_path_query(None, 'data', path)
        assert expr == "jsonb_path_query(data, '$.items[*]')"

    def test_jsonb_path_query_with_vars(self):
        """Test jsonb_path_query with variables."""
        expr = jsonb_path_query(None, 'data', '$.items[*]?(@.price < $max)', {'max': 100})
        assert "jsonb_path_query(data, '$.items[*]?(@.price < $max)'" in expr
        assert "'{\"max\": 100}'" in expr

    def test_jsonb_path_query_first_simple(self):
        """Test simple jsonb_path_query_first."""
        expr = jsonb_path_query_first(None, 'data', '$.items[0]')
        assert expr == "jsonb_path_query_first(data, '$.items[0]')"

    def test_jsonb_path_query_first_with_path_object(self):
        """Test jsonb_path_query_first with PostgresJsonPath."""
        path = PostgresJsonPath('$.items[0].name')
        expr = jsonb_path_query_first(None, 'data', path)
        assert expr == "jsonb_path_query_first(data, '$.items[0].name')"

    def test_jsonb_path_query_first_with_vars(self):
        """Test jsonb_path_query_first with variables."""
        expr = jsonb_path_query_first(None, 'data', '$.items[*]?(@.id == $id)', {'id': 1})
        assert "jsonb_path_query_first(data, '$.items[*]?(@.id == $id)'" in expr

    def test_jsonb_path_exists_simple(self):
        """Test simple jsonb_path_exists."""
        expr = jsonb_path_exists(None, 'data', '$.items[*]?(@.active)')
        assert expr == "jsonb_path_exists(data, '$.items[*]?(@.active)')"

    def test_jsonb_path_exists_with_path_object(self):
        """Test jsonb_path_exists with PostgresJsonPath."""
        path = PostgresJsonPath('$.name')
        expr = jsonb_path_exists(None, 'data', path)
        assert expr == "jsonb_path_exists(data, '$.name')"

    def test_jsonb_path_exists_with_vars(self):
        """Test jsonb_path_exists with variables."""
        expr = jsonb_path_exists(None, 'data', '$.items[*]?(@.price > $min)', {'min': 50})
        assert "jsonb_path_exists(data, '$.items[*]?(@.price > $min)'" in expr

    def test_jsonb_path_match_simple(self):
        """Test simple jsonb_path_match."""
        expr = jsonb_path_match(None, 'data', '$.items[*]?(@.active)')
        assert expr == "jsonb_path_match(data, '$.items[*]?(@.active)')"

    def test_jsonb_path_match_with_path_object(self):
        """Test jsonb_path_match with PostgresJsonPath."""
        path = PostgresJsonPath('$.isActive')
        expr = jsonb_path_match(None, 'data', path)
        assert expr == "jsonb_path_match(data, '$.isActive')"

    def test_jsonb_path_match_with_vars(self):
        """Test jsonb_path_match with variables."""
        expr = jsonb_path_match(None, 'data', '$.items[*]?(@.price < $max)', {'max': 100})
        assert "jsonb_path_match(data, '$.items[*]?(@.price < $max)'" in expr


class TestPostgresJsonPathAdapter:
    """Tests for PostgresJsonPathAdapter."""

    def test_adapter_supported_types(self):
        """Test supported types property."""
        adapter = PostgresJsonPathAdapter()
        supported = adapter.supported_types
        assert PostgresJsonPath in supported

    def test_to_database_path_object(self):
        """Test converting PostgresJsonPath to database."""
        adapter = PostgresJsonPathAdapter()
        path = PostgresJsonPath('$.items[*]')
        result = adapter.to_database(path, str)
        assert result == '$.items[*]'

    def test_to_database_string(self):
        """Test converting string to database."""
        adapter = PostgresJsonPathAdapter()
        result = adapter.to_database('$.items[*]', str)
        assert result == '$.items[*]'

    def test_to_database_none(self):
        """Test converting None to database."""
        adapter = PostgresJsonPathAdapter()
        result = adapter.to_database(None, str)
        assert result is None

    def test_to_database_invalid_string(self):
        """Test that invalid string raises ValueError."""
        adapter = PostgresJsonPathAdapter()
        with pytest.raises(ValueError, match="jsonpath must start with"):
            adapter.to_database('items[*]', str)

    def test_to_database_invalid_type(self):
        """Test that invalid type raises TypeError."""
        adapter = PostgresJsonPathAdapter()
        with pytest.raises(TypeError, match="Cannot convert"):
            adapter.to_database(123, str)

    def test_from_database_string(self):
        """Test converting string from database."""
        adapter = PostgresJsonPathAdapter()
        result = adapter.from_database('$.items[*]', PostgresJsonPath)
        assert isinstance(result, PostgresJsonPath)
        assert result.path == '$.items[*]'

    def test_from_database_path_object(self):
        """Test converting PostgresJsonPath object."""
        adapter = PostgresJsonPathAdapter()
        path = PostgresJsonPath('$.items[*]')
        result = adapter.from_database(path, PostgresJsonPath)
        assert result == path

    def test_from_database_none(self):
        """Test converting None from database."""
        adapter = PostgresJsonPathAdapter()
        result = adapter.from_database(None, PostgresJsonPath)
        assert result is None

    def test_from_database_invalid_type(self):
        """Test that invalid type raises TypeError."""
        adapter = PostgresJsonPathAdapter()
        with pytest.raises(TypeError, match="Cannot convert"):
            adapter.from_database(123, PostgresJsonPath)


class TestPostgresJsonPathAdapterBatch:
    """Tests for batch conversion methods."""

    def test_to_database_batch(self):
        """Test batch conversion to database."""
        adapter = PostgresJsonPathAdapter()
        values = [
            PostgresJsonPath('$.items[*]'),
            '$.store.book',
            None,
        ]
        results = adapter.to_database_batch(values, str)
        assert results[0] == '$.items[*]'
        assert results[1] == '$.store.book'
        assert results[2] is None

    def test_from_database_batch(self):
        """Test batch conversion from database."""
        adapter = PostgresJsonPathAdapter()
        values = ['$.items[*]', '$.store.book', None]
        results = adapter.from_database_batch(values, PostgresJsonPath)
        assert results[0].path == '$.items[*]'
        assert results[1].path == '$.store.book'
        assert results[2] is None

    def test_batch_empty_list(self):
        """Test batch conversion with empty list."""
        adapter = PostgresJsonPathAdapter()
        results = adapter.to_database_batch([], str)
        assert results == []

    def test_batch_mixed_types(self):
        """Test batch conversion with mixed valid types."""
        adapter = PostgresJsonPathAdapter()
        values = [
            PostgresJsonPath('$.a'),
            '$.b',
        ]
        results = adapter.to_database_batch(values, str)
        assert results == ['$.a', '$.b']
