# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_pgvector_types.py
"""Unit tests for PostgreSQL pgvector type.

Tests for:
- PostgresVector data class
- PostgresVectorAdapter conversion
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.types.pgvector import PostgresVector
from rhosocial.activerecord.backend.impl.postgres.adapters.pgvector import PostgresVectorAdapter


class TestPostgresVector:
    """Tests for PostgresVector data class."""

    def test_create_with_values(self):
        """Test creating vector from list of values."""
        v = PostgresVector([1.0, 2.0, 3.0])
        assert v.values == [1.0, 2.0, 3.0]
        assert v.dimensions == 3

    def test_create_with_explicit_dimensions(self):
        """Test creating vector with explicit dimension count."""
        v = PostgresVector([1.0, 2.0, 3.0], dimensions=3)
        assert v.dimensions == 3

    def test_dimension_mismatch_raises_error(self):
        """Test that dimension mismatch raises ValueError."""
        with pytest.raises(ValueError, match="Expected 4 dimensions, got 3"):
            PostgresVector([1.0, 2.0, 3.0], dimensions=4)

    def test_empty_vector_raises_error(self):
        """Test that empty vector raises ValueError."""
        with pytest.raises(ValueError, match="at least one dimension"):
            PostgresVector([])

    def test_to_postgres_string(self):
        """Test converting to PostgreSQL string format."""
        v = PostgresVector([1.0, 2.0, 3.0])
        assert v.to_postgres_string() == "[1.0, 2.0, 3.0]"

    def test_to_postgres_string_single(self):
        """Test single-element vector."""
        v = PostgresVector([5.5])
        assert v.to_postgres_string() == "[5.5]"

    def test_from_postgres_string(self):
        """Test parsing from PostgreSQL string."""
        v = PostgresVector.from_postgres_string("[1.0,2.0,3.0]")
        assert v.values == [1.0, 2.0, 3.0]
        assert v.dimensions == 3

    def test_from_postgres_string_with_spaces(self):
        """Test parsing with spaces."""
        v = PostgresVector.from_postgres_string("[1.0, 2.0, 3.0]")
        assert v.values == [1.0, 2.0, 3.0]

    def test_from_postgres_string_invalid_format(self):
        """Test that invalid format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid vector format"):
            PostgresVector.from_postgres_string("1.0,2.0,3.0")

    def test_from_postgres_string_empty(self):
        """Test that empty brackets raises ValueError."""
        with pytest.raises(ValueError, match="Empty vector"):
            PostgresVector.from_postgres_string("[]")

    def test_to_sql_literal(self):
        """Test SQL literal with type cast."""
        v = PostgresVector([1.0, 2.0, 3.0])
        assert v.to_sql_literal() == "[1.0, 2.0, 3.0]::vector(3)"

    def test_equality(self):
        """Test equality comparison."""
        v1 = PostgresVector([1.0, 2.0, 3.0])
        v2 = PostgresVector([1.0, 2.0, 3.0])
        assert v1 == v2

    def test_equality_with_list(self):
        """Test equality with list."""
        v = PostgresVector([1.0, 2.0, 3.0])
        assert v == [1.0, 2.0, 3.0]

    def test_inequality(self):
        """Test inequality."""
        v1 = PostgresVector([1.0, 2.0, 3.0])
        v2 = PostgresVector([4.0, 5.0, 6.0])
        assert v1 != v2

    def test_hash(self):
        """Test hashability."""
        v1 = PostgresVector([1.0, 2.0, 3.0])
        v2 = PostgresVector([1.0, 2.0, 3.0])
        assert hash(v1) == hash(v2)
        assert len({v1, v2}) == 1

    def test_len(self):
        """Test length."""
        v = PostgresVector([1.0, 2.0, 3.0])
        assert len(v) == 3

    def test_str(self):
        """Test string representation."""
        v = PostgresVector([1.0, 2.0, 3.0])
        assert str(v) == "[1.0, 2.0, 3.0]"

    def test_repr(self):
        """Test repr."""
        v = PostgresVector([1.0, 2.0, 3.0])
        r = repr(v)
        assert "PostgresVector" in r
        assert "[1.0, 2.0, 3.0]" in r

    def test_frozen(self):
        """Test that vector is immutable (frozen dataclass)."""
        v = PostgresVector([1.0, 2.0, 3.0])
        with pytest.raises(AttributeError):
            v.values = [4.0, 5.0, 6.0]

    def test_roundtrip(self):
        """Test roundtrip: create -> to_postgres_string -> from_postgres_string."""
        original = PostgresVector([1.5, 2.5, 3.5])
        s = original.to_postgres_string()
        restored = PostgresVector.from_postgres_string(s)
        assert restored == original


class TestPostgresVectorAdapter:
    """Tests for PostgresVectorAdapter."""

    def test_adapter_supported_types(self):
        """Test supported_types property."""
        adapter = PostgresVectorAdapter()
        supported = adapter.supported_types
        assert PostgresVector in supported

    def test_to_database_vector(self):
        """Test converting PostgresVector to database."""
        adapter = PostgresVectorAdapter()
        v = PostgresVector([1.0, 2.0, 3.0])
        result = adapter.to_database(v, str)
        assert result == "[1.0, 2.0, 3.0]"

    def test_to_database_list(self):
        """Test converting List[float] to database (convenience)."""
        adapter = PostgresVectorAdapter()
        result = adapter.to_database([1.0, 2.0, 3.0], str)
        assert result == "[1.0, 2.0, 3.0]"

    def test_to_database_string(self):
        """Test converting string to database (passthrough)."""
        adapter = PostgresVectorAdapter()
        result = adapter.to_database("[1.0, 2.0, 3.0]", str)
        assert result == "[1.0, 2.0, 3.0]"

    def test_to_database_none(self):
        """Test converting None to database."""
        adapter = PostgresVectorAdapter()
        result = adapter.to_database(None, str)
        assert result is None

    def test_to_database_invalid_type(self):
        """Test converting invalid type raises TypeError."""
        adapter = PostgresVectorAdapter()
        with pytest.raises(TypeError, match="Cannot convert"):
            adapter.to_database(12345, str)

    def test_from_database_string(self):
        """Test converting string from database."""
        adapter = PostgresVectorAdapter()
        result = adapter.from_database("[1.0,2.0,3.0]", PostgresVector)
        assert isinstance(result, PostgresVector)
        assert result.values == [1.0, 2.0, 3.0]

    def test_from_database_list(self):
        """Test converting list from database."""
        adapter = PostgresVectorAdapter()
        result = adapter.from_database([1.0, 2.0, 3.0], PostgresVector)
        assert isinstance(result, PostgresVector)
        assert result.values == [1.0, 2.0, 3.0]

    def test_from_database_vector(self):
        """Test converting PostgresVector from database."""
        adapter = PostgresVectorAdapter()
        v = PostgresVector([1.0, 2.0, 3.0])
        result = adapter.from_database(v, PostgresVector)
        assert result is v

    def test_from_database_none(self):
        """Test converting None from database."""
        adapter = PostgresVectorAdapter()
        result = adapter.from_database(None, PostgresVector)
        assert result is None

    def test_from_database_invalid_type(self):
        """Test converting invalid type from database raises TypeError."""
        adapter = PostgresVectorAdapter()
        with pytest.raises(TypeError, match="Cannot convert"):
            adapter.from_database(12345, PostgresVector)

    def test_batch_to_database(self):
        """Test batch conversion to database."""
        adapter = PostgresVectorAdapter()
        values = [
            PostgresVector([1.0, 2.0]),
            [3.0, 4.0],
            "[5.0,6.0]",
            None,
        ]
        results = adapter.to_database_batch(values, str)
        assert results[0] == "[1.0, 2.0]"
        assert results[1] == "[3.0, 4.0]"
        assert results[2] == "[5.0,6.0]"
        assert results[3] is None

    def test_batch_from_database(self):
        """Test batch conversion from database."""
        adapter = PostgresVectorAdapter()
        values = ["[1.0,2.0]", [3.0, 4.0], None]
        results = adapter.from_database_batch(values, PostgresVector)
        assert isinstance(results[0], PostgresVector)
        assert results[0].values == [1.0, 2.0]
        assert isinstance(results[1], PostgresVector)
        assert results[1].values == [3.0, 4.0]
        assert results[2] is None
