# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_citext_types.py
"""Unit tests for PostgreSQL citext type.

Tests for:
- PostgresCitext data class
- PostgresCitextAdapter conversion
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.types.citext import PostgresCitext
from rhosocial.activerecord.backend.impl.postgres.adapters.citext import PostgresCitextAdapter


class TestPostgresCitext:
    """Tests for PostgresCitext data class."""

    def test_create(self):
        c = PostgresCitext("Hello World")
        assert c.value == "Hello World"

    def test_to_postgres_string(self):
        c = PostgresCitext("Hello")
        assert c.to_postgres_string() == "Hello"

    def test_from_postgres_string(self):
        c = PostgresCitext.from_postgres_string("Hello")
        assert isinstance(c, PostgresCitext)
        assert c.value == "Hello"

    def test_to_sql_literal(self):
        c = PostgresCitext("Hello")
        assert c.to_sql_literal() == "'Hello'::citext"

    def test_to_sql_literal_with_quote(self):
        c = PostgresCitext("It's")
        assert c.to_sql_literal() == "'It''s'::citext"

    def test_equality_case_insensitive(self):
        a = PostgresCitext("Hello")
        b = PostgresCitext("hello")
        assert a == b

    def test_equality_with_string(self):
        c = PostgresCitext("Hello")
        assert c == "hello"

    def test_inequality(self):
        a = PostgresCitext("Hello")
        b = PostgresCitext("World")
        assert a != b

    def test_hash_case_insensitive(self):
        a = PostgresCitext("Hello")
        b = PostgresCitext("hello")
        assert hash(a) == hash(b)
        assert len({a, b}) == 1

    def test_str(self):
        c = PostgresCitext("Hello")
        assert str(c) == "Hello"

    def test_repr(self):
        c = PostgresCitext("Hello")
        r = repr(c)
        assert "PostgresCitext" in r

    def test_frozen(self):
        c = PostgresCitext("Hello")
        with pytest.raises(AttributeError):
            c.value = "World"


class TestPostgresCitextAdapter:
    """Tests for PostgresCitextAdapter."""

    def test_supported_types(self):
        adapter = PostgresCitextAdapter()
        supported = adapter.supported_types
        assert PostgresCitext in supported

    def test_to_database_citext(self):
        adapter = PostgresCitextAdapter()
        c = PostgresCitext("Hello")
        result = adapter.to_database(c, str)
        assert result == "Hello"

    def test_to_database_string(self):
        adapter = PostgresCitextAdapter()
        result = adapter.to_database("Hello", str)
        assert result == "Hello"

    def test_to_database_none(self):
        adapter = PostgresCitextAdapter()
        result = adapter.to_database(None, str)
        assert result is None

    def test_to_database_invalid_type(self):
        adapter = PostgresCitextAdapter()
        with pytest.raises(TypeError, match="Cannot convert"):
            adapter.to_database(123, str)

    def test_from_database_string(self):
        adapter = PostgresCitextAdapter()
        result = adapter.from_database("Hello", PostgresCitext)
        assert isinstance(result, PostgresCitext)
        assert result.value == "Hello"

    def test_from_database_citext(self):
        adapter = PostgresCitextAdapter()
        c = PostgresCitext("Hello")
        result = adapter.from_database(c, PostgresCitext)
        assert result is c

    def test_from_database_none(self):
        adapter = PostgresCitextAdapter()
        result = adapter.from_database(None, PostgresCitext)
        assert result is None

    def test_from_database_invalid_type(self):
        adapter = PostgresCitextAdapter()
        with pytest.raises(TypeError, match="Cannot convert"):
            adapter.from_database(123, PostgresCitext)

    def test_batch_to_database(self):
        adapter = PostgresCitextAdapter()
        values = [PostgresCitext("A"), "B", None]
        results = adapter.to_database_batch(values, str)
        assert results[0] == "A"
        assert results[1] == "B"
        assert results[2] is None

    def test_batch_from_database(self):
        adapter = PostgresCitextAdapter()
        values = ["Hello", "World", None]
        results = adapter.from_database_batch(values, PostgresCitext)
        assert isinstance(results[0], PostgresCitext)
        assert isinstance(results[1], PostgresCitext)
        assert results[2] is None