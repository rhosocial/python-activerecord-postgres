# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_macaddr8_types.py
"""Unit tests for PostgreSQL MACADDR8 type.

Tests for:
- PostgresMacaddr8 data class
- PostgresMacaddr8Adapter conversion
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.types.network_address import PostgresMacaddr8
from rhosocial.activerecord.backend.impl.postgres.adapters.network_address import PostgresMacaddr8Adapter


class TestPostgresMacaddr8:
    """Tests for PostgresMacaddr8 data class."""

    def test_create_from_colon_format(self):
        """Test creating from colon-separated format."""
        mac = PostgresMacaddr8('08:00:2b:01:02:03:04:05')
        assert mac.address == '08:00:2b:01:02:03:04:05'

    def test_create_from_hyphen_format(self):
        """Test creating from hyphen-separated format."""
        mac = PostgresMacaddr8('08-00-2b-01-02-03-04-05')
        assert mac.address == '08:00:2b:01:02:03:04:05'

    def test_create_from_no_separator(self):
        """Test creating from no-separator format."""
        mac = PostgresMacaddr8('08002b0102030405')
        assert mac.address == '08:00:2b:01:02:03:04:05'

    def test_create_from_mixed_format(self):
        """Test creating from mixed separator format."""
        mac = PostgresMacaddr8('08002b:0102030405')
        assert mac.address == '08:00:2b:01:02:03:04:05'

    def test_create_from_eui48_format(self):
        """Test creating from 6-byte EUI-48 format."""
        mac = PostgresMacaddr8('08:00:2b:01:02:03')
        assert mac.address == '08:00:2b:ff:fe:01:02:03'
        assert mac.is_eui48_derived is True

    def test_create_from_bytes(self):
        """Test creating from bytes."""
        mac = PostgresMacaddr8(bytes([0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05]))
        assert mac.address == '08:00:2b:01:02:03:04:05'

    def test_uppercase_input(self):
        """Test uppercase hex digits."""
        mac = PostgresMacaddr8('08:00:2B:01:02:03:04:05')
        assert mac.address == '08:00:2b:01:02:03:04:05'

    def test_whitespace_handling(self):
        """Test whitespace trimming."""
        mac = PostgresMacaddr8('  08:00:2b:01:02:03:04:05  ')
        assert mac.address == '08:00:2b:01:02:03:04:05'

    def test_invalid_length(self):
        """Test invalid MAC address length."""
        with pytest.raises(ValueError, match="Invalid MAC address length"):
            PostgresMacaddr8('08:00:2b')

    def test_invalid_hex(self):
        """Test invalid hex digits."""
        with pytest.raises(ValueError, match="Invalid hex digit"):
            PostgresMacaddr8('08:00:2b:01:02:03:04:gg')

    def test_string_representation(self):
        """Test string representation."""
        mac = PostgresMacaddr8('08:00:2b:01:02:03:04:05')
        assert str(mac) == '08:00:2b:01:02:03:04:05'

    def test_repr(self):
        """Test repr."""
        mac = PostgresMacaddr8('08:00:2b:01:02:03:04:05')
        assert '08:00:2b:01:02:03:04:05' in repr(mac)

    def test_equality_macaddr8(self):
        """Test equality with PostgresMacaddr8."""
        mac1 = PostgresMacaddr8('08:00:2b:01:02:03:04:05')
        mac2 = PostgresMacaddr8('08-00-2b-01-02-03-04-05')
        assert mac1 == mac2

    def test_equality_string(self):
        """Test equality with string."""
        mac = PostgresMacaddr8('08:00:2b:01:02:03:04:05')
        assert mac == '08:00:2b:01:02:03:04:05'
        assert mac == '08-00-2b-01-02-03-04-05'

    def test_inequality(self):
        """Test inequality."""
        mac1 = PostgresMacaddr8('08:00:2b:01:02:03:04:05')
        mac2 = PostgresMacaddr8('08:00:2b:01:02:03:04:06')
        assert mac1 != mac2

    def test_hash(self):
        """Test hashability."""
        mac1 = PostgresMacaddr8('08:00:2b:01:02:03:04:05')
        mac2 = PostgresMacaddr8('08:00:2b:01:02:03:04:05')
        assert hash(mac1) == hash(mac2)
        assert len({mac1, mac2}) == 1

    def test_to_bytes(self):
        """Test bytes conversion."""
        mac = PostgresMacaddr8('08:00:2b:01:02:03:04:05')
        result = bytes(mac)
        expected = b'\\x08\\x00\\x2b\\x01\\x02\\x03\\x04\\x05'
        assert len(result) == 8
        assert result[0] == 0x08
        assert result[1] == 0x00
        assert result[2] == 0x2b

    def test_is_eui48_derived_true(self):
        """Test is_eui48_derived for EUI-48 derived address."""
        mac = PostgresMacaddr8('08:00:2b:01:02:03')  # 6-byte input
        assert mac.is_eui48_derived is True

    def test_is_eui48_derived_false(self):
        """Test is_eui48_derived for native EUI-64 address."""
        mac = PostgresMacaddr8('08:00:2b:01:02:03:04:05')
        assert mac.is_eui48_derived is False

    def test_to_ipv6_interface(self):
        """Test IPv6 interface identifier generation."""
        mac = PostgresMacaddr8('08:00:2b:01:02:03:04:05')
        result = mac.to_ipv6_interface()
        assert result.startswith('fe80::')
        assert '0800:2b01' in result

    def test_to_ipv6_interface_custom_prefix(self):
        """Test IPv6 interface with custom prefix."""
        mac = PostgresMacaddr8('08:00:2b:01:02:03:04:05')
        result = mac.to_ipv6_interface(prefix='2001:db8::')
        assert result.startswith('2001:db8::')

    def test_set7bit(self):
        """Test setting the 7th bit."""
        mac = PostgresMacaddr8('08:00:2b:01:02:03:04:05')
        result = mac.set7bit()
        assert result.address.startswith('0a:')  # 0x08 | 0x02 = 0x0a
        assert result != mac  # Original unchanged

    def test_set7bit_already_set(self):
        """Test setting the 7th bit when already set."""
        mac = PostgresMacaddr8('0a:00:2b:01:02:03:04:05')  # 7th bit already set
        result = mac.set7bit()
        assert result.address == mac.address  # No change


class TestPostgresMacaddr8Adapter:
    """Tests for PostgresMacaddr8Adapter."""

    def test_adapter_supported_types(self):
        """Test supported types property."""
        adapter = PostgresMacaddr8Adapter()
        supported = adapter.supported_types
        assert PostgresMacaddr8 in supported

    def test_to_database_macaddr8(self):
        """Test converting PostgresMacaddr8 to database."""
        adapter = PostgresMacaddr8Adapter()
        mac = PostgresMacaddr8('08:00:2b:01:02:03:04:05')
        result = adapter.to_database(mac, str)
        assert result == '08:00:2b:01:02:03:04:05'

    def test_to_database_string(self):
        """Test converting string to database."""
        adapter = PostgresMacaddr8Adapter()
        result = adapter.to_database('08:00:2b:01:02:03:04:05', str)
        assert result == '08:00:2b:01:02:03:04:05'

    def test_to_database_bytes(self):
        """Test converting bytes to database."""
        adapter = PostgresMacaddr8Adapter()
        result = adapter.to_database(bytes([0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05]), str)
        assert result == '08:00:2b:01:02:03:04:05'

    def test_to_database_none(self):
        """Test converting None to database."""
        adapter = PostgresMacaddr8Adapter()
        result = adapter.to_database(None, str)
        assert result is None

    def test_to_database_invalid_type(self):
        """Test converting invalid type raises error."""
        adapter = PostgresMacaddr8Adapter()
        with pytest.raises(TypeError):
            adapter.to_database(123, str)

    def test_from_database_string(self):
        """Test converting string from database."""
        adapter = PostgresMacaddr8Adapter()
        result = adapter.from_database('08:00:2b:01:02:03:04:05', PostgresMacaddr8)
        assert isinstance(result, PostgresMacaddr8)
        assert result.address == '08:00:2b:01:02:03:04:05'

    def test_from_database_macaddr8(self):
        """Test converting PostgresMacaddr8 from database."""
        adapter = PostgresMacaddr8Adapter()
        mac = PostgresMacaddr8('08:00:2b:01:02:03:04:05')
        result = adapter.from_database(mac, PostgresMacaddr8)
        assert result is mac

    def test_from_database_none(self):
        """Test converting None from database."""
        adapter = PostgresMacaddr8Adapter()
        result = adapter.from_database(None, PostgresMacaddr8)
        assert result is None

    def test_from_database_invalid_type(self):
        """Test converting invalid type from database."""
        adapter = PostgresMacaddr8Adapter()
        with pytest.raises(TypeError):
            adapter.from_database(123, PostgresMacaddr8)

    def test_batch_to_database(self):
        """Test batch conversion to database."""
        adapter = PostgresMacaddr8Adapter()
        values = [
            PostgresMacaddr8('08:00:2b:01:02:03:04:05'),
            '08:00:2b:01:02:03:04:06',
            None
        ]
        results = adapter.to_database_batch(values, str)
        assert results[0] == '08:00:2b:01:02:03:04:05'
        assert results[1] == '08:00:2b:01:02:03:04:06'
        assert results[2] is None

    def test_batch_from_database(self):
        """Test batch conversion from database."""
        adapter = PostgresMacaddr8Adapter()
        values = ['08:00:2b:01:02:03:04:05', '08:00:2b:01:02:03:04:06', None]
        results = adapter.from_database_batch(values, PostgresMacaddr8)
        assert results[0].address == '08:00:2b:01:02:03:04:05'
        assert results[1].address == '08:00:2b:01:02:03:04:06'
        assert results[2] is None
