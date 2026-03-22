
import ipaddress
import json
import pytest
from psycopg.types.json import Jsonb

from rhosocial.activerecord.backend.impl.postgres.adapters import PostgresJSONBAdapter, \
    PostgresNetworkAddressAdapter


# Tests for PostgresJSONBAdapter
def test_jsonb_adapter_supported_types():
    adapter = PostgresJSONBAdapter()
    supported = adapter.supported_types
    assert supported[dict] == [Jsonb]


def test_jsonb_to_database():
    adapter = PostgresJSONBAdapter()
    my_dict = {"a": 1, "b": [2, 3]}
    my_list = [1, "test", {"c": 4}]

    # Test with dict
    result_dict = adapter.to_database(my_dict, dict)
    assert isinstance(result_dict, Jsonb)
    assert result_dict.obj == my_dict

    # Test with list
    result_list = adapter.to_database(my_list, list)
    assert isinstance(result_list, Jsonb)
    assert result_list.obj == my_list

    # Test with None
    assert adapter.to_database(None, dict) is None


def test_jsonb_from_database():
    adapter = PostgresJSONBAdapter()
    my_dict = {"key": "value"}
    my_list = ["item1", "item2"]
    json_string = '{"key": "value_from_string"}'

    # Test with value already being a dict or list
    assert adapter.from_database(my_dict, dict) is my_dict
    assert adapter.from_database(my_list, list) is my_list

    # Test with a JSON string representation
    result = adapter.from_database(json_string, dict)
    assert isinstance(result, dict)
    assert result["key"] == "value_from_string"

    # Test with None
    assert adapter.from_database(None, dict) is None


# Tests for PostgresNetworkAddressAdapter
def test_network_adapter_supported_types():
    adapter = PostgresNetworkAddressAdapter()
    supported = adapter.supported_types
    assert supported[ipaddress.IPv4Address] == [str]
    assert supported[ipaddress.IPv6Address] == [str]
    assert supported[ipaddress.IPv4Network] == [str]
    assert supported[ipaddress.IPv6Network] == [str]


def test_network_adapter_supported_types_no_ipaddress(mocker):
    """Test that it returns empty dict if ipaddress module is missing."""
    mocker.patch('builtins.__import__', side_effect=ImportError)
    adapter = PostgresNetworkAddressAdapter()
    assert adapter.supported_types == {}


@pytest.mark.parametrize("ip_obj, expected_str", [
    (ipaddress.ip_address("192.168.1.1"), "192.168.1.1"),
    (ipaddress.ip_address("2001:db8::1"), "2001:db8::1"),
    (ipaddress.ip_network("192.168.0.0/24"), "192.168.0.0/24"),
    (ipaddress.ip_network("2001:db8::/32"), "2001:db8::/32"),
])
def test_network_to_database(ip_obj, expected_str):
    adapter = PostgresNetworkAddressAdapter()
    result = adapter.to_database(ip_obj, type(ip_obj))
    assert result == expected_str


def test_network_to_database_none():
    adapter = PostgresNetworkAddressAdapter()
    assert adapter.to_database(None, ipaddress.IPv4Address) is None


@pytest.mark.parametrize("ip_str, expected_type", [
    ("192.168.1.1", ipaddress.IPv4Address),
    ("2001:db8::1", ipaddress.IPv6Address),
    ("192.168.0.0/24", ipaddress.IPv4Network),
    ("2001:db8::/32", ipaddress.IPv6Network),
])
def test_network_from_database(ip_str, expected_type):
    adapter = PostgresNetworkAddressAdapter()
    result = adapter.from_database(ip_str, expected_type)
    assert isinstance(result, expected_type)
    assert str(result) == ip_str


def test_network_from_database_fallback():
    """Test fallback behavior for invalid network strings."""
    adapter = PostgresNetworkAddressAdapter()
    invalid_str = "not-an-ip"
    result = adapter.from_database(invalid_str, str)
    assert result == invalid_str


def test_network_from_database_none():
    adapter = PostgresNetworkAddressAdapter()
    assert adapter.from_database(None, ipaddress.IPv4Address) is None

def test_network_from_database_no_ipaddress(mocker):
    """Test that it returns the original value if ipaddress module is missing."""
    mocker.patch('builtins.__import__', side_effect=ImportError)
    adapter = PostgresNetworkAddressAdapter()
    ip_str = "192.168.1.1"
    assert adapter.from_database(ip_str, str) == ip_str


# =============================================================================
# Batch Operation Tests
# =============================================================================

from rhosocial.activerecord.backend.impl.postgres.adapters.base import (
    PostgresListAdapter,
    PostgresJSONBAdapter,
    PostgresNetworkAddressAdapter,
)


class TestPostgresListAdapterBatch:
    """Test PostgresListAdapter batch methods."""

    def test_to_database_batch_pass_through(self):
        """Test that to_database_batch returns the same list (pass-through)."""
        adapter = PostgresListAdapter()
        test_data = [[1, 2, 3], [4, 5, 6], None, [7, 8, 9]]
        result = adapter.to_database_batch(test_data, list)
        # Should be the same object (no copy)
        assert result is test_data

    def test_to_database_batch_empty_list(self):
        """Test batch conversion with empty list."""
        adapter = PostgresListAdapter()
        result = adapter.to_database_batch([], list)
        assert result == []

    def test_from_database_batch_pass_through(self):
        """Test that from_database_batch returns the same list."""
        adapter = PostgresListAdapter()
        test_data = [[1, 2], [3, 4], None]
        result = adapter.from_database_batch(test_data, list)
        assert result is test_data

    def test_from_database_batch_empty_list(self):
        """Test batch conversion from database with empty list."""
        adapter = PostgresListAdapter()
        result = adapter.from_database_batch([], list)
        assert result == []


class TestPostgresJSONBAdapterBatch:
    """Test PostgresJSONBAdapter batch methods."""

    def test_to_database_batch(self):
        """Test batch conversion to database format."""
        adapter = PostgresJSONBAdapter()
        test_data = [
            {'a': 1},
            {'b': 2},
            None,
            {'c': [1, 2, 3]}
        ]
        result = adapter.to_database_batch(test_data, dict)

        assert len(result) == 4
        assert isinstance(result[0], Jsonb)
        assert result[0].obj == {'a': 1}
        assert isinstance(result[1], Jsonb)
        assert result[1].obj == {'b': 2}
        assert result[2] is None
        assert isinstance(result[3], Jsonb)
        assert result[3].obj == {'c': [1, 2, 3]}

    def test_to_database_batch_empty_list(self):
        """Test batch conversion with empty list."""
        adapter = PostgresJSONBAdapter()
        result = adapter.to_database_batch([], dict)
        assert result == []

    def test_from_database_batch_to_dict(self):
        """Test batch conversion from database to dict."""
        adapter = PostgresJSONBAdapter()
        test_data = [
            {'a': 1},
            None,
            {'b': 2}
        ]
        result = adapter.from_database_batch(test_data, dict)

        assert len(result) == 3
        assert result[0] == {'a': 1}
        assert result[1] is None
        assert result[2] == {'b': 2}

    def test_from_database_batch_to_str(self):
        """Test batch conversion from database to JSON string."""
        adapter = PostgresJSONBAdapter()
        test_data = [
            {'a': 1},
            None,
            {'b': [2, 3]}
        ]
        result = adapter.from_database_batch(test_data, str)

        assert len(result) == 3
        assert result[0] == '{"a": 1}'
        assert result[1] is None
        assert result[2] == '{"b": [2, 3]}'

    def test_from_database_batch_empty_list(self):
        """Test batch conversion from database with empty list."""
        adapter = PostgresJSONBAdapter()
        result = adapter.from_database_batch([], dict)
        assert result == []


class TestPostgresNetworkAddressAdapterBatch:
    """Test PostgresNetworkAddressAdapter batch methods."""

    def test_to_database_batch(self):
        """Test batch conversion to database format."""
        adapter = PostgresNetworkAddressAdapter()
        test_data = [
            ipaddress.IPv4Address('192.168.1.1'),
            ipaddress.IPv6Address('::1'),
            None,
            ipaddress.IPv4Network('10.0.0.0/24')
        ]
        result = adapter.to_database_batch(test_data, str)

        assert len(result) == 4
        assert result[0] == '192.168.1.1'
        assert result[1] == '::1'
        assert result[2] is None
        assert result[3] == '10.0.0.0/24'

    def test_to_database_batch_empty_list(self):
        """Test batch conversion with empty list."""
        adapter = PostgresNetworkAddressAdapter()
        result = adapter.to_database_batch([], str)
        assert result == []

    def test_from_database_batch(self):
        """Test batch conversion from database."""
        adapter = PostgresNetworkAddressAdapter()
        test_data = [
            '192.168.1.1',
            None,
            '10.0.0.0/24',
            '::1'
        ]
        result = adapter.from_database_batch(test_data, str)

        assert len(result) == 4
        assert isinstance(result[0], ipaddress.IPv4Address)
        assert str(result[0]) == '192.168.1.1'
        assert result[1] is None
        assert isinstance(result[2], ipaddress.IPv4Network)
        assert str(result[2]) == '10.0.0.0/24'
        assert isinstance(result[3], ipaddress.IPv6Address)
        assert str(result[3]) == '::1'

    def test_from_database_batch_invalid_values(self):
        """Test batch conversion with invalid values falls back to string."""
        adapter = PostgresNetworkAddressAdapter()
        test_data = [
            '192.168.1.1',
            'not-an-ip',
            '10.0.0.0/24'
        ]
        result = adapter.from_database_batch(test_data, str)

        assert len(result) == 3
        assert isinstance(result[0], ipaddress.IPv4Address)
        assert result[1] == 'not-an-ip'  # Falls back to original string
        assert isinstance(result[2], ipaddress.IPv4Network)

    def test_from_database_batch_empty_list(self):
        """Test batch conversion from database with empty list."""
        adapter = PostgresNetworkAddressAdapter()
        result = adapter.from_database_batch([], str)
        assert result == []


class TestPostgresEnumAdapterBatch:
    """Test PostgresEnumAdapter batch methods."""

    def test_to_database_batch(self):
        """Test batch conversion to database format."""
        from rhosocial.activerecord.backend.impl.postgres.adapters.base import PostgresEnumAdapter
        from enum import Enum

        class TestEnum(Enum):
            A = 1
            B = 2
            C = 3

        adapter = PostgresEnumAdapter()
        test_data = [
            TestEnum.A,
            'existing_string',
            None,
            TestEnum.C
        ]
        result = adapter.to_database_batch(test_data, str)

        assert len(result) == 4
        assert result[0] == 'A'
        assert result[1] == 'existing_string'
        assert result[2] is None
        assert result[3] == 'C'

    def test_from_database_batch(self):
        """Test batch conversion from database."""
        from rhosocial.activerecord.backend.impl.postgres.adapters.base import PostgresEnumAdapter

        adapter = PostgresEnumAdapter()
        test_data = ['value1', None, 'value2']
        result = adapter.from_database_batch(test_data, str)

        assert len(result) == 3
        assert result[0] == 'value1'
        assert result[1] is None
        assert result[2] == 'value2'

    def test_from_database_batch_to_enum(self):
        """Test batch conversion from database to Python Enum."""
        from rhosocial.activerecord.backend.impl.postgres.adapters.base import PostgresEnumAdapter
        from enum import Enum

        class TestEnum(Enum):
            A = 1
            B = 2

        adapter = PostgresEnumAdapter()
        test_data = ['A', 'B', None]
        result = adapter.from_database_batch(test_data, str, {'enum_class': TestEnum})

        assert len(result) == 3
        assert result[0] == TestEnum.A
        assert result[1] == TestEnum.B
        assert result[2] is None
