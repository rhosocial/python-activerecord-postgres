
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
    assert supported[list] == [Jsonb]


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
