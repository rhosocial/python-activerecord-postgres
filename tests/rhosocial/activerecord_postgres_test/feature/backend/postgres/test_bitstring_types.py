# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_bitstring_types.py
"""
Unit tests for PostgreSQL bit string types.

Tests for:
- PostgresBitString data class
- PostgresBitStringAdapter conversion
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.types.bit_string import PostgresBitString
from rhosocial.activerecord.backend.impl.postgres.adapters.bit_string import PostgresBitStringAdapter


class TestPostgresBitString:
    """Tests for PostgresBitString data class."""

    def test_create_bitstring(self):
        """Test creating a bit string."""
        bs = PostgresBitString('10101010')
        assert bs.bits == '10101010'
        assert bs.length is None

    def test_create_bitstring_with_length(self):
        """Test creating a fixed-length bit string."""
        bs = PostgresBitString('1010', length=8)
        assert bs.bits == '10100000'  # Padded with zeros
        assert bs.length == 8

    def test_invalid_bitstring(self):
        """Test that invalid characters raise error."""
        with pytest.raises(ValueError):
            PostgresBitString('10102')  # Contains '2'

    def test_bitstring_to_postgres_string(self):
        """Test bit string to PostgreSQL literal conversion."""
        bs = PostgresBitString('10101')
        assert bs.to_postgres_string() == "B'10101'"

    def test_bitstring_from_postgres_string(self):
        """Test parsing PostgreSQL bit string literal."""
        bs = PostgresBitString.from_postgres_string("B'10101'")
        assert bs.bits == '10101'

    def test_bitstring_from_plain_string(self):
        """Test parsing plain bit string."""
        bs = PostgresBitString.from_postgres_string('10101')
        assert bs.bits == '10101'

    def test_bitstring_from_int(self):
        """Test creating bit string from integer."""
        bs = PostgresBitString.from_int(42)
        assert bs.bits == '101010'
        assert int(bs) == 42

    def test_bitstring_from_int_with_length(self):
        """Test creating bit string from integer with fixed length."""
        bs = PostgresBitString.from_int(5, length=8)
        assert bs.bits == '00000101'
        assert len(bs) == 8

    def test_bitstring_from_negative_int(self):
        """Test that negative integers raise error."""
        with pytest.raises(ValueError):
            PostgresBitString.from_int(-1)

    def test_bitstring_str(self):
        """Test string conversion."""
        bs = PostgresBitString('10101')
        assert str(bs) == '10101'

    def test_bitstring_int(self):
        """Test integer conversion."""
        bs = PostgresBitString('10101')
        assert int(bs) == 21

    def test_bitstring_len(self):
        """Test length."""
        bs = PostgresBitString('10101')
        assert len(bs) == 5

    def test_bitstring_equality(self):
        """Test equality comparison."""
        bs1 = PostgresBitString('10101')
        bs2 = PostgresBitString('10101')
        bs3 = PostgresBitString('10100')
        assert bs1 == bs2
        assert bs1 != bs3

    def test_bitstring_equality_with_str(self):
        """Test equality with string."""
        bs = PostgresBitString('10101')
        assert bs == '10101'
        assert bs != '10100'

    def test_bitstring_equality_with_int(self):
        """Test equality with integer."""
        bs = PostgresBitString('10101')
        assert bs == 21  # 10101 in binary
        assert bs != 20

    def test_bitstring_hash(self):
        """Test hashability."""
        bs1 = PostgresBitString('10101')
        bs2 = PostgresBitString('10101')
        assert hash(bs1) == hash(bs2)
        assert len({bs1, bs2}) == 1


class TestPostgresBitStringAdapter:
    """Tests for PostgresBitStringAdapter."""

    def test_adapter_supported_types(self):
        """Test supported types property."""
        adapter = PostgresBitStringAdapter()
        supported = adapter.supported_types
        assert PostgresBitString in supported
        # supported_types is a Dict[Type, Set[Type]] mapping Python types to SQL types
        # PostgresBitString maps to {str}
        assert str in supported[PostgresBitString]

    def test_to_database_bitstring(self):
        """Test converting PostgresBitString to database."""
        adapter = PostgresBitStringAdapter()
        bs = PostgresBitString('10101')
        result = adapter.to_database(bs, str)
        assert result == "B'10101'"

    def test_to_database_none(self):
        """Test converting None to database."""
        adapter = PostgresBitStringAdapter()
        result = adapter.to_database(None, str)
        assert result is None

    def test_to_database_string(self):
        """Test converting string to database."""
        adapter = PostgresBitStringAdapter()
        result = adapter.to_database('10101', str)
        assert result == "B'10101'"

    def test_to_database_invalid_string(self):
        """Test that invalid string raises error."""
        adapter = PostgresBitStringAdapter()
        with pytest.raises(ValueError):
            adapter.to_database('10102', str)

    def test_to_database_int(self):
        """Test converting integer to database."""
        adapter = PostgresBitStringAdapter()
        result = adapter.to_database(21, str)
        assert result == "B'10101'"

    def test_to_database_int_with_length(self):
        """Test converting integer with fixed length."""
        adapter = PostgresBitStringAdapter()
        result = adapter.to_database(5, str, {'length': 8})
        assert result == "B'00000101'"

    def test_from_database_bitstring(self):
        """Test converting PostgresBitString from database."""
        adapter = PostgresBitStringAdapter()
        bs = PostgresBitString('10101')
        result = adapter.from_database(bs, PostgresBitString)
        assert result == bs

    def test_from_database_none(self):
        """Test converting None from database."""
        adapter = PostgresBitStringAdapter()
        result = adapter.from_database(None, PostgresBitString)
        assert result is None

    def test_from_database_string(self):
        """Test converting string from database."""
        adapter = PostgresBitStringAdapter()
        result = adapter.from_database('10101', PostgresBitString)
        assert isinstance(result, PostgresBitString)
        assert result.bits == '10101'

    def test_from_database_int(self):
        """Test converting integer from database."""
        adapter = PostgresBitStringAdapter()
        result = adapter.from_database(21, PostgresBitString)
        assert isinstance(result, PostgresBitString)
        assert int(result) == 21

    def test_batch_to_database(self):
        """Test batch conversion to database."""
        adapter = PostgresBitStringAdapter()
        values = [
            PostgresBitString('10101'),
            '11011',
            42,
            None,
        ]
        results = adapter.to_database_batch(values, str)
        assert results[0] == "B'10101'"
        assert results[1] == "B'11011'"
        assert results[2] == "B'101010'"
        assert results[3] is None

    def test_batch_from_database(self):
        """Test batch conversion from database."""
        adapter = PostgresBitStringAdapter()
        values = ['10101', '11011', None]
        results = adapter.from_database_batch(values, PostgresBitString)
        assert results[0].bits == '10101'
        assert results[1].bits == '11011'
        assert results[2] is None


class TestPostgresBitStringOperations:
    """Tests for bit string operations."""

    def test_zero_value(self):
        """Test zero bit string."""
        bs = PostgresBitString.from_int(0)
        assert bs.bits == '0'
        assert int(bs) == 0

    def test_large_value(self):
        """Test large integer value."""
        value = 2**32 - 1  # Max 32-bit unsigned
        bs = PostgresBitString.from_int(value)
        assert int(bs) == value
        assert len(bs.bits) == 32

    def test_empty_bitstring(self):
        """Test empty bit string."""
        bs = PostgresBitString('')
        assert bs.bits == ''
        assert len(bs) == 0
        assert int(bs) == 0
