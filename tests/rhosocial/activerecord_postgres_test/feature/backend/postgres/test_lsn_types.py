# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_lsn_types.py
"""Unit tests for PostgreSQL pg_lsn type.

Tests for:
- PostgresLsn data class
- PostgresLsnAdapter conversion
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.types.pg_lsn import PostgresLsn
from rhosocial.activerecord.backend.impl.postgres.adapters.pg_lsn import PostgresLsnAdapter


class TestPostgresLsn:
    """Tests for PostgresLsn data class."""

    def test_create_from_string(self):
        """Test creating LSN from string format."""
        lsn = PostgresLsn.from_string('16/B374D848')
        assert lsn.value == (0x16 << 32) | 0xB374D848

    def test_create_from_string_lower_case(self):
        """Test creating LSN from lowercase string."""
        lsn = PostgresLsn.from_string('16/b374d848')
        assert lsn.value == (0x16 << 32) | 0xB374D848

    def test_create_from_string_mixed_case(self):
        """Test creating LSN from mixed case string."""
        lsn = PostgresLsn.from_string('16/B374d848')
        assert lsn.value == (0x16 << 32) | 0xB374D848

    def test_create_from_string_with_whitespace(self):
        """Test creating LSN from string with whitespace."""
        lsn = PostgresLsn.from_string('  16/B374D848  ')
        assert lsn.value == (0x16 << 32) | 0xB374D848

    def test_create_from_string_zero(self):
        """Test creating LSN from zero value."""
        lsn = PostgresLsn.from_string('0/0')
        assert lsn.value == 0

    def test_create_from_string_max(self):
        """Test creating LSN from max value."""
        lsn = PostgresLsn.from_string('FFFFFFFF/FFFFFFFF')
        assert lsn.value == 0xFFFFFFFFFFFFFFFF

    def test_create_from_string_invalid_format(self):
        """Test creating LSN from invalid format raises error."""
        with pytest.raises(ValueError, match="Invalid LSN format"):
            PostgresLsn.from_string('invalid')

    def test_create_from_string_invalid_no_slash(self):
        """Test creating LSN without slash raises error."""
        with pytest.raises(ValueError, match="Invalid LSN format"):
            PostgresLsn.from_string('16B374D848')

    def test_create_from_string_invalid_hex(self):
        """Test creating LSN from invalid hex raises error."""
        with pytest.raises(ValueError, match="Invalid LSN format"):
            PostgresLsn.from_string('GG/12345678')

    def test_create_from_integer(self):
        """Test creating LSN from integer."""
        lsn = PostgresLsn(123456789)
        assert lsn.value == 123456789

    def test_create_from_integer_zero(self):
        """Test creating LSN from zero."""
        lsn = PostgresLsn(0)
        assert lsn.value == 0

    def test_create_from_integer_max(self):
        """Test creating LSN from max 64-bit value."""
        lsn = PostgresLsn(0xFFFFFFFFFFFFFFFF)
        assert lsn.value == 0xFFFFFFFFFFFFFFFF

    def test_create_from_integer_negative(self):
        """Test creating LSN from negative value raises error."""
        with pytest.raises(ValueError, match="LSN value must be non-negative"):
            PostgresLsn(-1)

    def test_create_from_integer_overflow(self):
        """Test creating LSN from value exceeding 64-bit raises error."""
        with pytest.raises(ValueError, match="LSN value exceeds 64-bit range"):
            PostgresLsn(0xFFFFFFFFFFFFFFFF + 1)

    def test_create_from_non_integer(self):
        """Test creating LSN from non-integer raises error."""
        with pytest.raises(TypeError, match="LSN value must be int"):
            PostgresLsn('not an int')

    def test_equality_lsn(self):
        """Test equality between two LSNs."""
        lsn1 = PostgresLsn.from_string('16/B374D848')
        lsn2 = PostgresLsn.from_string('16/B374D848')
        assert lsn1 == lsn2

    def test_equality_string(self):
        """Test equality with string."""
        lsn = PostgresLsn.from_string('16/B374D848')
        assert lsn == '16/B374D848'

    def test_equality_integer(self):
        """Test equality with integer."""
        lsn = PostgresLsn(123456789)
        assert lsn == 123456789

    def test_inequality_lsn(self):
        """Test inequality between two LSNs."""
        lsn1 = PostgresLsn.from_string('16/B374D848')
        lsn2 = PostgresLsn.from_string('16/B374D849')
        assert lsn1 != lsn2

    def test_inequality_string(self):
        """Test inequality with string."""
        lsn = PostgresLsn.from_string('16/B374D848')
        assert lsn != '16/B374D849'

    def test_less_than(self):
        """Test less than comparison."""
        lsn1 = PostgresLsn.from_string('16/B374D848')
        lsn2 = PostgresLsn.from_string('16/B374D849')
        assert lsn1 < lsn2
        assert not lsn2 < lsn1

    def test_less_than_integer(self):
        """Test less than comparison with integer."""
        lsn = PostgresLsn(100)
        assert lsn < 200
        assert not lsn < 50

    def test_less_than_or_equal(self):
        """Test less than or equal comparison."""
        lsn1 = PostgresLsn.from_string('16/B374D848')
        lsn2 = PostgresLsn.from_string('16/B374D848')
        lsn3 = PostgresLsn.from_string('16/B374D849')
        assert lsn1 <= lsn2
        assert lsn1 <= lsn3
        assert not lsn3 <= lsn1

    def test_greater_than(self):
        """Test greater than comparison."""
        lsn1 = PostgresLsn.from_string('16/B374D848')
        lsn2 = PostgresLsn.from_string('16/B374D849')
        assert lsn2 > lsn1
        assert not lsn1 > lsn2

    def test_greater_than_integer(self):
        """Test greater than comparison with integer."""
        lsn = PostgresLsn(100)
        assert lsn > 50
        assert not lsn > 200

    def test_greater_than_or_equal(self):
        """Test greater than or equal comparison."""
        lsn1 = PostgresLsn.from_string('16/B374D848')
        lsn2 = PostgresLsn.from_string('16/B374D848')
        lsn3 = PostgresLsn.from_string('16/B374D849')
        assert lsn1 >= lsn2
        assert lsn3 >= lsn1
        assert not lsn1 >= lsn3

    def test_subtraction_lsn(self):
        """Test subtraction between two LSNs."""
        lsn1 = PostgresLsn(1000)
        lsn2 = PostgresLsn(300)
        result = lsn1 - lsn2
        assert result == 700
        assert isinstance(result, int)

    def test_subtraction_integer(self):
        """Test subtraction with integer."""
        lsn = PostgresLsn(1000)
        result = lsn - 300
        assert result == 700

    def test_subtraction_reverse(self):
        """Test reverse subtraction (int - LSN)."""
        lsn = PostgresLsn(300)
        result = 1000 - lsn
        assert result == 700

    def test_subtraction_negative_result(self):
        """Test subtraction resulting in negative value."""
        lsn1 = PostgresLsn(100)
        lsn2 = PostgresLsn(300)
        result = lsn1 - lsn2
        assert result == -200

    def test_addition(self):
        """Test addition of bytes to LSN."""
        lsn = PostgresLsn(1000)
        result = lsn + 500
        assert isinstance(result, PostgresLsn)
        assert result.value == 1500

    def test_addition_reverse(self):
        """Test reverse addition (int + LSN)."""
        lsn = PostgresLsn(1000)
        result = 500 + lsn
        assert isinstance(result, PostgresLsn)
        assert result.value == 1500

    def test_addition_zero(self):
        """Test adding zero."""
        lsn = PostgresLsn(1000)
        result = lsn + 0
        assert result.value == 1000

    def test_addition_overflow(self):
        """Test addition that would overflow raises error."""
        lsn = PostgresLsn(0xFFFFFFFFFFFFFFFF)
        with pytest.raises(ValueError, match="LSN addition exceeds 64-bit range"):
            lsn + 1

    def test_addition_negative_result(self):
        """Test addition resulting in negative value raises error."""
        lsn = PostgresLsn(100)
        with pytest.raises(ValueError, match="LSN addition would result in negative value"):
            lsn + (-200)

    def test_string_representation(self):
        """Test string representation."""
        lsn = PostgresLsn.from_string('16/B374D848')
        assert str(lsn) == '16/B374D848'

    def test_string_representation_zero(self):
        """Test string representation of zero."""
        lsn = PostgresLsn(0)
        assert str(lsn) == '0/00000000'

    def test_repr(self):
        """Test repr."""
        lsn = PostgresLsn.from_string('16/B374D848')
        assert 'PostgresLsn' in repr(lsn)
        assert '16/B374D848' in repr(lsn)

    def test_hash(self):
        """Test hashability."""
        lsn1 = PostgresLsn.from_string('16/B374D848')
        lsn2 = PostgresLsn.from_string('16/B374D848')
        assert hash(lsn1) == hash(lsn2)
        assert len({lsn1, lsn2}) == 1

    def test_hash_different(self):
        """Test hash of different LSNs."""
        lsn1 = PostgresLsn(100)
        lsn2 = PostgresLsn(200)
        assert hash(lsn1) != hash(lsn2)

    def test_int_conversion(self):
        """Test integer conversion."""
        lsn = PostgresLsn(123456789)
        assert int(lsn) == 123456789

    def test_high_property(self):
        """Test high property (segment number)."""
        lsn = PostgresLsn.from_string('16/B374D848')
        assert lsn.high == 0x16

    def test_low_property(self):
        """Test low property (byte offset)."""
        lsn = PostgresLsn.from_string('16/B374D848')
        assert lsn.low == 0xB374D848


class TestPostgresLsnAdapter:
    """Tests for PostgresLsnAdapter."""

    def test_adapter_supported_types(self):
        """Test supported types property."""
        adapter = PostgresLsnAdapter()
        supported = adapter.supported_types
        assert PostgresLsn in supported

    def test_to_database_lsn(self):
        """Test converting PostgresLsn to database."""
        adapter = PostgresLsnAdapter()
        lsn = PostgresLsn.from_string('16/B374D848')
        result = adapter.to_database(lsn, str)
        assert result == '16/B374D848'

    def test_to_database_integer(self):
        """Test converting integer to database."""
        adapter = PostgresLsnAdapter()
        result = adapter.to_database(123456789, str)
        assert result == str(PostgresLsn(123456789))

    def test_to_database_string(self):
        """Test converting string to database."""
        adapter = PostgresLsnAdapter()
        result = adapter.to_database('16/B374D848', str)
        assert result == '16/B374D848'

    def test_to_database_none(self):
        """Test converting None to database."""
        adapter = PostgresLsnAdapter()
        result = adapter.to_database(None, str)
        assert result is None

    def test_to_database_invalid_type(self):
        """Test converting invalid type raises error."""
        adapter = PostgresLsnAdapter()
        with pytest.raises(TypeError):
            adapter.to_database([1, 2, 3], str)

    def test_from_database_string(self):
        """Test converting string from database."""
        adapter = PostgresLsnAdapter()
        result = adapter.from_database('16/B374D848', PostgresLsn)
        assert isinstance(result, PostgresLsn)
        assert result.value == (0x16 << 32) | 0xB374D848

    def test_from_database_integer(self):
        """Test converting integer from database."""
        adapter = PostgresLsnAdapter()
        result = adapter.from_database(123456789, PostgresLsn)
        assert isinstance(result, PostgresLsn)
        assert result.value == 123456789

    def test_from_database_lsn(self):
        """Test converting PostgresLsn from database."""
        adapter = PostgresLsnAdapter()
        lsn = PostgresLsn.from_string('16/B374D848')
        result = adapter.from_database(lsn, PostgresLsn)
        assert result is lsn

    def test_from_database_none(self):
        """Test converting None from database."""
        adapter = PostgresLsnAdapter()
        result = adapter.from_database(None, PostgresLsn)
        assert result is None

    def test_from_database_invalid_type(self):
        """Test converting invalid type from database."""
        adapter = PostgresLsnAdapter()
        with pytest.raises(TypeError):
            adapter.from_database([1, 2, 3], PostgresLsn)

    def test_batch_to_database(self):
        """Test batch conversion to database."""
        adapter = PostgresLsnAdapter()
        lsn = PostgresLsn.from_string('16/B374D848')
        values = [lsn, '16/B374D849', 123456, None]
        results = adapter.to_database_batch(values, str)
        assert results[0] == '16/B374D848'
        assert results[1] == '16/B374D849'
        assert results[2] == str(PostgresLsn(123456))
        assert results[3] is None

    def test_batch_from_database(self):
        """Test batch conversion from database."""
        adapter = PostgresLsnAdapter()
        values = ['16/B374D848', '16/B374D849', None]
        results = adapter.from_database_batch(values, PostgresLsn)
        assert isinstance(results[0], PostgresLsn)
        assert results[0].value == (0x16 << 32) | 0xB374D848
        assert isinstance(results[1], PostgresLsn)
        assert results[1].value == (0x16 << 32) | 0xB374D849
        assert results[2] is None

    def test_batch_from_database_with_integers(self):
        """Test batch conversion from database with integers."""
        adapter = PostgresLsnAdapter()
        values = [123456789, 987654321, None]
        results = adapter.from_database_batch(values, PostgresLsn)
        assert isinstance(results[0], PostgresLsn)
        assert results[0].value == 123456789
        assert isinstance(results[1], PostgresLsn)
        assert results[1].value == 987654321
        assert results[2] is None
