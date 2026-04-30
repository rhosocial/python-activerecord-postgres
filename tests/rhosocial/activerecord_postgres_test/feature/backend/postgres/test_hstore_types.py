# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_hstore_types.py
"""Unit tests for PostgreSQL hstore type.

Tests for:
- PostgresHstore data class
- PostgresHstoreAdapter conversion
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.types.hstore import PostgresHstore
from rhosocial.activerecord.backend.impl.postgres.adapters.hstore import PostgresHstoreAdapter


class TestPostgresHstore:
    """Tests for PostgresHstore data class."""

    def test_create_from_dict(self):
        """Test creating hstore from a dictionary."""
        h = PostgresHstore(data={"name": "Alice", "age": "30"})
        assert h.data == {"name": "Alice", "age": "30"}
        assert len(h) == 2

    def test_create_empty(self):
        """Test creating empty hstore."""
        h = PostgresHstore(data={})
        assert len(h) == 0
        assert not h

    def test_create_none_data(self):
        """Test creating hstore with None data defaults to empty dict."""
        h = PostgresHstore(data=None)
        assert h.data == {}
        assert len(h) == 0

    def test_create_with_null_value(self):
        """Test creating hstore with NULL value."""
        h = PostgresHstore(data={"name": "Alice", "email": None})
        assert h["email"] is None

    def test_getitem(self):
        """Test key access via __getitem__."""
        h = PostgresHstore(data={"name": "Alice"})
        assert h["name"] == "Alice"

    def test_contains(self):
        """Test key existence via __contains__."""
        h = PostgresHstore(data={"name": "Alice"})
        assert "name" in h
        assert "age" not in h

    def test_keys_values_items(self):
        """Test keys(), values(), items() methods."""
        h = PostgresHstore(data={"a": "1", "b": "2"})
        assert h.keys() == ["a", "b"]
        assert h.values() == ["1", "2"]
        assert list(h.items()) == [("a", "1"), ("b", "2")]

    def test_to_postgres_string(self):
        """Test converting to PostgreSQL hstore literal string."""
        h = PostgresHstore(data={"a": "1", "b": "2"})
        result = h.to_postgres_string()
        assert '"a"=>"1"' in result
        assert '"b"=>"2"' in result

    def test_to_postgres_string_empty(self):
        """Test converting empty hstore to string."""
        h = PostgresHstore(data={})
        assert h.to_postgres_string() == ""

    def test_to_postgres_string_null_value(self):
        """Test converting hstore with NULL value to string."""
        h = PostgresHstore(data={"name": "Alice", "email": None})
        result = h.to_postgres_string()
        assert '"name"=>"Alice"' in result
        assert '"email"=>NULL' in result

    def test_to_postgres_string_escape_quotes(self):
        """Test escaping quotes in values."""
        h = PostgresHstore(data={"msg": 'He said "hello"'})
        result = h.to_postgres_string()
        assert r"\"hello\"" in result

    def test_from_postgres_string(self):
        """Test parsing hstore string from PostgreSQL."""
        h = PostgresHstore.from_postgres_string('"name"=>"Alice", "age"=>"30"')
        assert h.data == {"name": "Alice", "age": "30"}

    def test_from_postgres_string_empty(self):
        """Test parsing empty hstore string."""
        h = PostgresHstore.from_postgres_string("")
        assert h.data == {}

    def test_from_postgres_string_null_value(self):
        """Test parsing hstore string with NULL value."""
        h = PostgresHstore.from_postgres_string('"name"=>"Alice", "email"=>NULL')
        assert h.data == {"name": "Alice", "email": None}

    def test_from_postgres_string_invalid(self):
        """Test that invalid hstore string raises ValueError."""
        with pytest.raises(ValueError):
            PostgresHstore.from_postgres_string("not valid hstore")

    def test_equality(self):
        """Test equality."""
        h1 = PostgresHstore(data={"a": "1", "b": "2"})
        h2 = PostgresHstore(data={"a": "1", "b": "2"})
        assert h1 == h2

    def test_equality_with_dict(self):
        """Test equality with plain dict."""
        h = PostgresHstore(data={"a": "1"})
        assert h == {"a": "1"}

    def test_inequality(self):
        """Test inequality."""
        h1 = PostgresHstore(data={"a": "1"})
        h2 = PostgresHstore(data={"b": "2"})
        assert h1 != h2

    def test_hash(self):
        """Test hashability."""
        h1 = PostgresHstore(data={"a": "1", "b": "2"})
        h2 = PostgresHstore(data={"a": "1", "b": "2"})
        assert hash(h1) == hash(h2)

    def test_str(self):
        """Test string representation."""
        h = PostgresHstore(data={"a": "1"})
        assert '"a"=>"1"' in str(h)

    def test_frozen(self):
        """Test that hstore is immutable (frozen dataclass)."""
        h = PostgresHstore(data={"a": "1"})
        with pytest.raises(AttributeError):
            h.data = {"b": "2"}

    def test_roundtrip(self):
        """Test to_postgres_string -> from_postgres_string roundtrip."""
        original = PostgresHstore(data={"name": "Alice", "age": "30"})
        s = original.to_postgres_string()
        restored = PostgresHstore.from_postgres_string(s)
        assert original == restored


class TestPostgresHstoreAdapter:
    """Tests for PostgresHstoreAdapter."""

    def test_adapter_supported_types(self):
        """Test supported_types property."""
        adapter = PostgresHstoreAdapter()
        supported = adapter.supported_types
        assert PostgresHstore in supported
        assert dict in supported

    def test_to_database_hstore(self):
        """Test converting PostgresHstore to database."""
        adapter = PostgresHstoreAdapter()
        h = PostgresHstore(data={"a": "1", "b": "2"})
        result = adapter.to_database(h, str)
        assert '"a"=>"1"' in result
        assert '"b"=>"2"' in result

    def test_to_database_dict(self):
        """Test converting dict to database (convenience)."""
        adapter = PostgresHstoreAdapter()
        result = adapter.to_database({"a": "1"}, str)
        assert '"a"=>"1"' in result

    def test_to_database_string(self):
        """Test converting string to database (passthrough)."""
        adapter = PostgresHstoreAdapter()
        result = adapter.to_database('"key"=>"value"', str)
        assert result == '"key"=>"value"'

    def test_to_database_none(self):
        """Test converting None to database."""
        adapter = PostgresHstoreAdapter()
        result = adapter.to_database(None, str)
        assert result is None

    def test_to_database_invalid_type(self):
        """Test converting invalid type raises TypeError."""
        adapter = PostgresHstoreAdapter()
        with pytest.raises(TypeError, match="Cannot convert"):
            adapter.to_database(12345, str)

    def test_from_database_string(self):
        """Test converting string from database."""
        adapter = PostgresHstoreAdapter()
        result = adapter.from_database('"name"=>"Alice"', PostgresHstore)
        assert isinstance(result, PostgresHstore)
        assert result.data == {"name": "Alice"}

    def test_from_database_hstore(self):
        """Test converting PostgresHstore from database (passthrough)."""
        adapter = PostgresHstoreAdapter()
        h = PostgresHstore(data={"a": "1"})
        result = adapter.from_database(h, PostgresHstore)
        assert result is h

    def test_from_database_dict(self):
        """Test converting dict from database."""
        adapter = PostgresHstoreAdapter()
        result = adapter.from_database({"a": "1"}, PostgresHstore)
        assert isinstance(result, PostgresHstore)
        assert result.data == {"a": "1"}

    def test_from_database_none(self):
        """Test converting None from database."""
        adapter = PostgresHstoreAdapter()
        result = adapter.from_database(None, PostgresHstore)
        assert result is None

    def test_from_database_invalid_type(self):
        """Test converting invalid type from database raises TypeError."""
        adapter = PostgresHstoreAdapter()
        with pytest.raises(TypeError, match="Cannot convert"):
            adapter.from_database(12345, PostgresHstore)

    def test_batch_to_database(self):
        """Test batch conversion to database."""
        adapter = PostgresHstoreAdapter()
        values = [
            PostgresHstore(data={"a": "1"}),
            {"b": "2"},
            None,
        ]
        results = adapter.to_database_batch(values, str)
        assert '"a"=>"1"' in results[0]
        assert '"b"=>"2"' in results[1]
        assert results[2] is None

    def test_batch_from_database(self):
        """Test batch conversion from database."""
        adapter = PostgresHstoreAdapter()
        values = ['"name"=>"Alice"', '"age"=>"30"', None]
        results = adapter.from_database_batch(values, PostgresHstore)
        assert isinstance(results[0], PostgresHstore)
        assert results[0].data == {"name": "Alice"}
        assert isinstance(results[1], PostgresHstore)
        assert results[2] is None
