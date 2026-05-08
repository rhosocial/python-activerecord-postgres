# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_ltree_types.py
"""Unit tests for PostgreSQL ltree type.

Tests for:
- PostgresLtree data class
- PostgresLquery data class
- PostgresLtxtquery data class
- PostgresLtreeAdapter conversion
- PostgresLqueryAdapter conversion
- PostgresLtxtqueryAdapter conversion
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.types.ltree import (
    PostgresLtree,
    PostgresLquery,
    PostgresLtxtquery,
)
from rhosocial.activerecord.backend.impl.postgres.adapters.ltree import (
    PostgresLtreeAdapter,
    PostgresLqueryAdapter,
    PostgresLtxtqueryAdapter,
)


class TestPostgresLtree:
    """Tests for PostgresLtree data class."""

    def test_create_from_labels(self):
        path = PostgresLtree(labels=["Top", "Science", "Astronomy"])
        assert path.labels == ["Top", "Science", "Astronomy"]
        assert len(path) == 3

    def test_create_single_label(self):
        path = PostgresLtree(labels=["Top"])
        assert path.labels == ["Top"]
        assert len(path) == 1

    def test_empty_labels_raises_error(self):
        with pytest.raises(ValueError, match="at least one label"):
            PostgresLtree(labels=[])

    def test_invalid_label_character(self):
        with pytest.raises(ValueError, match="Invalid character"):
            PostgresLtree(labels=["Top", "Sci.ence"])

    def test_to_postgres_string(self):
        path = PostgresLtree(labels=["Top", "Science", "Astronomy"])
        assert path.to_postgres_string() == "Top.Science.Astronomy"

    def test_from_postgres_string(self):
        path = PostgresLtree.from_postgres_string("Top.Science.Astronomy")
        assert path.labels == ["Top", "Science", "Astronomy"]

    def test_from_postgres_string_single(self):
        path = PostgresLtree.from_postgres_string("Top")
        assert path.labels == ["Top"]

    def test_from_postgres_string_with_spaces(self):
        path = PostgresLtree.from_postgres_string("  Top.Science  ")
        assert path.labels == ["Top", "Science"]

    def test_from_postgres_string_empty_raises_error(self):
        with pytest.raises(ValueError, match="Cannot parse empty"):
            PostgresLtree.from_postgres_string("")

    def test_to_sql_literal(self):
        path = PostgresLtree(labels=["Top", "Science"])
        assert path.to_sql_literal() == "'Top.Science'::ltree"

    def test_nlevel(self):
        path = PostgresLtree(labels=["Top", "Science", "Astronomy"])
        assert path.nlevel == 3

    def test_subpath_with_length(self):
        path = PostgresLtree(labels=["A", "B", "C", "D"])
        sub = path.subpath(0, 2)
        assert sub.labels == ["A", "B"]

    def test_subpath_without_length(self):
        path = PostgresLtree(labels=["A", "B", "C", "D"])
        sub = path.subpath(2)
        assert sub.labels == ["C", "D"]

    def test_is_ancestor_of(self):
        parent = PostgresLtree(labels=["Top", "Science"])
        child = PostgresLtree(labels=["Top", "Science", "Astronomy"])
        assert parent.is_ancestor_of(child)
        assert not child.is_ancestor_of(parent)

    def test_is_ancestor_of_equal(self):
        a = PostgresLtree(labels=["Top", "Science"])
        b = PostgresLtree(labels=["Top", "Science"])
        assert a.is_ancestor_of(b)

    def test_is_descendant_of(self):
        parent = PostgresLtree(labels=["Top", "Science"])
        child = PostgresLtree(labels=["Top", "Science", "Astronomy"])
        assert child.is_descendant_of(parent)
        assert not parent.is_descendant_of(child)

    def test_equality(self):
        a = PostgresLtree(labels=["Top", "Science"])
        b = PostgresLtree(labels=["Top", "Science"])
        assert a == b

    def test_equality_with_string(self):
        path = PostgresLtree(labels=["Top", "Science"])
        assert path == "Top.Science"

    def test_inequality(self):
        a = PostgresLtree(labels=["Top", "Science"])
        b = PostgresLtree(labels=["Top", "History"])
        assert a != b

    def test_hash(self):
        a = PostgresLtree(labels=["Top", "Science"])
        b = PostgresLtree(labels=["Top", "Science"])
        assert hash(a) == hash(b)
        assert len({a, b}) == 1

    def test_str(self):
        path = PostgresLtree(labels=["Top", "Science"])
        assert str(path) == "Top.Science"

    def test_repr(self):
        path = PostgresLtree(labels=["Top"])
        r = repr(path)
        assert "PostgresLtree" in r
        assert "Top" in r

    def test_frozen(self):
        path = PostgresLtree(labels=["Top"])
        with pytest.raises(AttributeError):
            path.labels = ["Other"]

    def test_roundtrip(self):
        original = PostgresLtree(labels=["Top", "Science", "Astronomy"])
        s = original.to_postgres_string()
        restored = PostgresLtree.from_postgres_string(s)
        assert restored == original


class TestPostgresLquery:
    """Tests for PostgresLquery data class."""

    def test_create(self):
        q = PostgresLquery(pattern="*.Astronomy.*")
        assert q.pattern == "*.Astronomy.*"

    def test_empty_raises_error(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            PostgresLquery(pattern="")

    def test_to_postgres_string(self):
        q = PostgresLquery(pattern="*.Astronomy.*")
        assert q.to_postgres_string() == "*.Astronomy.*"

    def test_to_sql_literal(self):
        q = PostgresLquery(pattern="*.Astronomy.*")
        assert q.to_sql_literal() == "'*.Astronomy.*'::lquery"

    def test_str(self):
        q = PostgresLquery(pattern="*.Astronomy.*")
        assert str(q) == "*.Astronomy.*"

    def test_equality(self):
        a = PostgresLquery(pattern="*.Astronomy.*")
        b = PostgresLquery(pattern="*.Astronomy.*")
        assert a == b

    def test_equality_with_string(self):
        q = PostgresLquery(pattern="*.Astronomy.*")
        assert q == "*.Astronomy.*"

    def test_hash(self):
        a = PostgresLquery(pattern="*.Astronomy.*")
        b = PostgresLquery(pattern="*.Astronomy.*")
        assert hash(a) == hash(b)


class TestPostgresLtxtquery:
    """Tests for PostgresLtxtquery data class."""

    def test_create(self):
        q = PostgresLtxtquery(query="Science & Astronomy")
        assert q.query == "Science & Astronomy"

    def test_empty_raises_error(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            PostgresLtxtquery(query="")

    def test_to_postgres_string(self):
        q = PostgresLtxtquery(query="Science & Astronomy")
        assert q.to_postgres_string() == "Science & Astronomy"

    def test_to_sql_literal(self):
        q = PostgresLtxtquery(query="Science & Astronomy")
        assert q.to_sql_literal() == "'Science & Astronomy'::ltxtquery"

    def test_str(self):
        q = PostgresLtxtquery(query="Science & Astronomy")
        assert str(q) == "Science & Astronomy"

    def test_equality(self):
        a = PostgresLtxtquery(query="Science & Astronomy")
        b = PostgresLtxtquery(query="Science & Astronomy")
        assert a == b

    def test_equality_with_string(self):
        q = PostgresLtxtquery(query="Science & Astronomy")
        assert q == "Science & Astronomy"

    def test_hash(self):
        a = PostgresLtxtquery(query="Science & Astronomy")
        b = PostgresLtxtquery(query="Science & Astronomy")
        assert hash(a) == hash(b)


class TestPostgresLtreeAdapter:
    """Tests for PostgresLtreeAdapter."""

    def test_supported_types(self):
        adapter = PostgresLtreeAdapter()
        supported = adapter.supported_types
        assert PostgresLtree in supported

    def test_to_database_ltree(self):
        adapter = PostgresLtreeAdapter()
        path = PostgresLtree(labels=["Top", "Science"])
        result = adapter.to_database(path, str)
        assert result == "Top.Science"

    def test_to_database_string(self):
        adapter = PostgresLtreeAdapter()
        result = adapter.to_database("Top.Science", str)
        assert result == "Top.Science"

    def test_to_database_none(self):
        adapter = PostgresLtreeAdapter()
        result = adapter.to_database(None, str)
        assert result is None

    def test_to_database_invalid_type(self):
        adapter = PostgresLtreeAdapter()
        with pytest.raises(TypeError, match="Cannot convert"):
            adapter.to_database(12345, str)

    def test_from_database_string(self):
        adapter = PostgresLtreeAdapter()
        result = adapter.from_database("Top.Science", PostgresLtree)
        assert isinstance(result, PostgresLtree)
        assert result.labels == ["Top", "Science"]

    def test_from_database_ltree(self):
        adapter = PostgresLtreeAdapter()
        path = PostgresLtree(labels=["Top"])
        result = adapter.from_database(path, PostgresLtree)
        assert result is path

    def test_from_database_none(self):
        adapter = PostgresLtreeAdapter()
        result = adapter.from_database(None, PostgresLtree)
        assert result is None

    def test_from_database_invalid_type(self):
        adapter = PostgresLtreeAdapter()
        with pytest.raises(TypeError, match="Cannot convert"):
            adapter.from_database(12345, PostgresLtree)

    def test_batch_to_database(self):
        adapter = PostgresLtreeAdapter()
        values = [
            PostgresLtree(labels=["A", "B"]),
            "C.D",
            None,
        ]
        results = adapter.to_database_batch(values, str)
        assert results[0] == "A.B"
        assert results[1] == "C.D"
        assert results[2] is None

    def test_batch_from_database(self):
        adapter = PostgresLtreeAdapter()
        values = ["A.B", "C.D", None]
        results = adapter.from_database_batch(values, PostgresLtree)
        assert isinstance(results[0], PostgresLtree)
        assert results[0].labels == ["A", "B"]
        assert isinstance(results[1], PostgresLtree)
        assert results[2] is None


class TestPostgresLqueryAdapter:
    """Tests for PostgresLqueryAdapter."""

    def test_to_database_lquery(self):
        adapter = PostgresLqueryAdapter()
        q = PostgresLquery(pattern="*.Astronomy.*")
        result = adapter.to_database(q, str)
        assert result == "*.Astronomy.*"

    def test_from_database_string(self):
        adapter = PostgresLqueryAdapter()
        result = adapter.from_database("*.Astronomy.*", PostgresLquery)
        assert isinstance(result, PostgresLquery)
        assert result.pattern == "*.Astronomy.*"


class TestPostgresLtxtqueryAdapter:
    """Tests for PostgresLtxtqueryAdapter."""

    def test_to_database_ltxtquery(self):
        adapter = PostgresLtxtqueryAdapter()
        q = PostgresLtxtquery(query="Science & Astronomy")
        result = adapter.to_database(q, str)
        assert result == "Science & Astronomy"

    def test_from_database_string(self):
        adapter = PostgresLtxtqueryAdapter()
        result = adapter.from_database("Science & Astronomy", PostgresLtxtquery)
        assert isinstance(result, PostgresLtxtquery)
        assert result.query == "Science & Astronomy"