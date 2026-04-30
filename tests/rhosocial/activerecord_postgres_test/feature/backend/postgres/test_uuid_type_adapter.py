# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_uuid_type_adapter.py
"""Unit tests for PostgreSQL UUID type adapter.

Tests for:
- PostgresUUIDAdapter bidirectional conversion
- uuid.UUID <-> str conversion
- None handling
- Invalid type handling
- Batch operations
"""
import uuid

import pytest

from rhosocial.activerecord.backend.impl.postgres.adapters.uuid import PostgresUUIDAdapter


class TestPostgresUUIDAdapter:
    """Tests for PostgresUUIDAdapter."""

    def test_adapter_supported_types(self):
        """Test supported_types property returns uuid.UUID mapping."""
        adapter = PostgresUUIDAdapter()
        supported = adapter.supported_types
        assert uuid.UUID in supported
        assert str in supported[uuid.UUID]

    def test_to_database_uuid(self):
        """Test converting uuid.UUID to database string."""
        adapter = PostgresUUIDAdapter()
        u = uuid.UUID('550e8400-e29b-41d4-a716-446655440000')
        result = adapter.to_database(u, str)
        assert result == '550e8400-e29b-41d4-a716-446655440000'
        assert isinstance(result, str)

    def test_to_database_uuid_v4(self):
        """Test converting uuid4 to database string."""
        adapter = PostgresUUIDAdapter()
        u = uuid.uuid4()
        result = adapter.to_database(u, str)
        assert result == str(u)

    def test_to_database_string(self):
        """Test converting valid UUID string to database."""
        adapter = PostgresUUIDAdapter()
        result = adapter.to_database('550e8400-e29b-41d4-a716-446655440000', str)
        assert result == '550e8400-e29b-41d4-a716-446655440000'

    def test_to_database_string_uppercase(self):
        """Test converting uppercase UUID string (validated and returned as-is)."""
        adapter = PostgresUUIDAdapter()
        result = adapter.to_database('550E8400-E29B-41D4-A716-446655440000', str)
        assert result == '550E8400-E29B-41D4-A716-446655440000'

    def test_to_database_string_no_hyphens(self):
        """Test converting UUID string without hyphens."""
        adapter = PostgresUUIDAdapter()
        result = adapter.to_database('550e8400e29b41d4a716446655440000', str)
        assert result == '550e8400e29b41d4a716446655440000'

    def test_to_database_none(self):
        """Test converting None to database returns None."""
        adapter = PostgresUUIDAdapter()
        result = adapter.to_database(None, str)
        assert result is None

    def test_to_database_invalid_type(self):
        """Test converting invalid type raises TypeError."""
        adapter = PostgresUUIDAdapter()
        with pytest.raises(TypeError, match="Cannot convert"):
            adapter.to_database(12345, str)

    def test_to_database_invalid_uuid_string(self):
        """Test converting invalid UUID string raises ValueError."""
        adapter = PostgresUUIDAdapter()
        with pytest.raises(ValueError):
            adapter.to_database('not-a-uuid', str)

    def test_from_database_string(self):
        """Test converting database string to uuid.UUID."""
        adapter = PostgresUUIDAdapter()
        result = adapter.from_database('550e8400-e29b-41d4-a716-446655440000', uuid.UUID)
        assert isinstance(result, uuid.UUID)
        assert result == uuid.UUID('550e8400-e29b-41d4-a716-446655440000')

    def test_from_database_uuid(self):
        """Test converting uuid.UUID from database returns same object."""
        adapter = PostgresUUIDAdapter()
        u = uuid.UUID('550e8400-e29b-41d4-a716-446655440000')
        result = adapter.from_database(u, uuid.UUID)
        assert result is u

    def test_from_database_none(self):
        """Test converting None from database returns None."""
        adapter = PostgresUUIDAdapter()
        result = adapter.from_database(None, uuid.UUID)
        assert result is None

    def test_from_database_invalid_type(self):
        """Test converting invalid type from database raises TypeError."""
        adapter = PostgresUUIDAdapter()
        with pytest.raises(TypeError, match="Cannot convert"):
            adapter.from_database(12345, uuid.UUID)

    def test_from_database_invalid_uuid_string(self):
        """Test converting invalid UUID string from database raises ValueError."""
        adapter = PostgresUUIDAdapter()
        with pytest.raises(ValueError):
            adapter.from_database('not-a-uuid', uuid.UUID)

    def test_roundtrip_uuid(self):
        """Test roundtrip: uuid.UUID -> to_database -> from_database -> uuid.UUID."""
        adapter = PostgresUUIDAdapter()
        original = uuid.UUID('550e8400-e29b-41d4-a716-446655440000')
        db_value = adapter.to_database(original, str)
        result = adapter.from_database(db_value, uuid.UUID)
        assert result == original

    def test_roundtrip_random_uuid(self):
        """Test roundtrip with random uuid4."""
        adapter = PostgresUUIDAdapter()
        original = uuid.uuid4()
        db_value = adapter.to_database(original, str)
        result = adapter.from_database(db_value, uuid.UUID)
        assert result == original

    def test_batch_to_database(self):
        """Test batch conversion to database."""
        adapter = PostgresUUIDAdapter()
        u1 = uuid.UUID('550e8400-e29b-41d4-a716-446655440000')
        u2 = uuid.uuid4()
        values = [u1, u2, '550e8400-e29b-41d4-a716-446655440001', None]
        results = adapter.to_database_batch(values, str)
        assert results[0] == str(u1)
        assert results[1] == str(u2)
        assert results[2] == '550e8400-e29b-41d4-a716-446655440001'
        assert results[3] is None

    def test_batch_from_database(self):
        """Test batch conversion from database."""
        adapter = PostgresUUIDAdapter()
        values = [
            '550e8400-e29b-41d4-a716-446655440000',
            '550e8400-e29b-41d4-a716-446655440001',
            None,
        ]
        results = adapter.from_database_batch(values, uuid.UUID)
        assert isinstance(results[0], uuid.UUID)
        assert results[0] == uuid.UUID('550e8400-e29b-41d4-a716-446655440000')
        assert isinstance(results[1], uuid.UUID)
        assert results[1] == uuid.UUID('550e8400-e29b-41d4-a716-446655440001')
        assert results[2] is None

    def test_batch_roundtrip(self):
        """Test batch roundtrip conversion."""
        adapter = PostgresUUIDAdapter()
        originals = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]
        db_values = adapter.to_database_batch(originals, str)
        results = adapter.from_database_batch(db_values, uuid.UUID)
        for original, result in zip(originals, results):
            assert result == original
