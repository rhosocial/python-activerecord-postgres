# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/types/test_network_address_integration.py
"""
Integration tests for PostgreSQL network address types with real database.

These tests require a live PostgreSQL connection and test:
- inet and cidr storage and retrieval
- macaddr and macaddr8 storage and retrieval
- Network address adapters output accepted by PostgreSQL
- Sync/async round-trip behavior
"""
import ipaddress

import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.impl.postgres.adapters.network_address import (
    PostgresMacaddr8Adapter,
    PostgresMacaddrAdapter,
    PostgresNetworkAddressAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.types.network_address import (
    PostgresMacaddr,
    PostgresMacaddr8,
)


NETWORK_TABLE = "test_network_address_types"
ASYNC_NETWORK_TABLE = "test_network_address_types_async"


def _inet_literal(value):
    adapter = PostgresNetworkAddressAdapter()
    database_value = adapter.to_database(value, str)
    if database_value is None:
        return "NULL"
    return f"'{database_value}'::inet"


def _cidr_literal(value):
    adapter = PostgresNetworkAddressAdapter()
    database_value = adapter.to_database(value, str)
    if database_value is None:
        return "NULL"
    return f"'{database_value}'::cidr"


def _macaddr_literal(value):
    adapter = PostgresMacaddrAdapter()
    database_value = adapter.to_database(value, str)
    if database_value is None:
        return "NULL"
    return f"'{database_value}'::macaddr"


def _macaddr8_literal(value):
    adapter = PostgresMacaddr8Adapter()
    database_value = adapter.to_database(value, str)
    if database_value is None:
        return "NULL"
    return f"'{database_value}'::macaddr8"


class TestSyncNetworkAddressIntegration:
    """Synchronous integration tests for PostgreSQL network address types."""

    @pytest.fixture
    def network_test_table(self, postgres_backend):
        """Create a table containing network address columns for sync tests."""
        postgres_backend.execute(f"DROP TABLE IF EXISTS {NETWORK_TABLE}")
        postgres_backend.execute(f"""
            CREATE TABLE {NETWORK_TABLE} (
                id SERIAL PRIMARY KEY,
                inet_value inet,
                cidr_value cidr,
                mac_value macaddr
            )
        """)
        yield NETWORK_TABLE
        postgres_backend.execute(f"DROP TABLE IF EXISTS {NETWORK_TABLE}")

    def test_insert_and_select_inet_value(self, postgres_backend, network_test_table):
        """Insert an IPv4 address and verify PostgreSQL returns the inet text."""
        literal = _inet_literal(ipaddress.ip_address("192.168.1.10"))

        postgres_backend.execute(
            f"INSERT INTO {network_test_table} (inet_value) VALUES ({literal})"
        )
        result = postgres_backend.fetch_one(
            f"SELECT host(inet_value) AS inet_value FROM {network_test_table} WHERE id = 1"
        )

        assert result["inet_value"] == "192.168.1.10"

    def test_insert_and_select_inet_with_mask(self, postgres_backend, network_test_table):
        """Insert an inet host with mask and verify the mask survives round-trip."""
        literal = _inet_literal("192.168.1.10/24")

        postgres_backend.execute(
            f"INSERT INTO {network_test_table} (inet_value) VALUES ({literal})"
        )
        result = postgres_backend.fetch_one(
            f"SELECT inet_value::text AS inet_value FROM {network_test_table} WHERE id = 1"
        )

        assert result["inet_value"] == "192.168.1.10/24"

    def test_insert_and_select_cidr_value(self, postgres_backend, network_test_table):
        """Insert a CIDR network and verify PostgreSQL returns canonical CIDR text."""
        literal = _cidr_literal(ipaddress.ip_network("192.168.1.0/24"))

        postgres_backend.execute(
            f"INSERT INTO {network_test_table} (cidr_value) VALUES ({literal})"
        )
        result = postgres_backend.fetch_one(
            f"SELECT cidr_value::text AS cidr_value FROM {network_test_table} WHERE id = 1"
        )

        assert result["cidr_value"] == "192.168.1.0/24"

    def test_insert_and_select_macaddr_value(self, postgres_backend, network_test_table):
        """Insert a MAC address and verify PostgreSQL returns canonical macaddr text."""
        literal = _macaddr_literal(PostgresMacaddr("08-00-2b-01-02-03"))

        postgres_backend.execute(
            f"INSERT INTO {network_test_table} (mac_value) VALUES ({literal})"
        )
        result = postgres_backend.fetch_one(
            f"SELECT mac_value::text AS mac_value FROM {network_test_table} WHERE id = 1"
        )

        assert result["mac_value"] == "08:00:2b:01:02:03"

    def test_insert_and_select_macaddr8_value(self, postgres_backend, network_test_table):
        """Insert a MACADDR8 value and verify PostgreSQL returns canonical text."""
        if postgres_backend.get_server_version() < (10, 0, 0):
            pytest.skip("macaddr8 requires PostgreSQL 10+")

        literal = _macaddr8_literal(PostgresMacaddr8("08:00:2b:01:02:03:04:05"))

        result = postgres_backend.fetch_one(
            f"SELECT {literal}::text AS mac8_value"
        )

        assert result["mac8_value"] == "08:00:2b:01:02:03:04:05"

    def test_network_null_round_trip(self, postgres_backend, network_test_table):
        """Insert NULL network address values and verify fetched values are None."""
        postgres_backend.execute(f"""
            INSERT INTO {network_test_table}
                (inet_value, cidr_value, mac_value)
            VALUES (NULL, NULL, NULL)
        """)
        result = postgres_backend.fetch_one(f"""
            SELECT inet_value, cidr_value, mac_value
            FROM {network_test_table}
            WHERE id = 1
        """)

        assert result["inet_value"] is None
        assert result["cidr_value"] is None
        assert result["mac_value"] is None

    def test_invalid_macaddr_rejected_before_insert(self):
        """Pass an invalid MAC address and verify the adapter rejects it before SQL."""
        with pytest.raises(ValueError):
            _macaddr_literal("08:00:2b")


class TestAsyncNetworkAddressIntegration:
    """Asynchronous integration tests for PostgreSQL network address types."""

    @pytest_asyncio.fixture
    async def async_network_test_table(self, async_postgres_backend):
        """Create a table containing network address columns for async tests."""
        await async_postgres_backend.execute(f"DROP TABLE IF EXISTS {ASYNC_NETWORK_TABLE}")
        await async_postgres_backend.execute(f"""
            CREATE TABLE {ASYNC_NETWORK_TABLE} (
                id SERIAL PRIMARY KEY,
                inet_value inet,
                cidr_value cidr,
                mac_value macaddr
            )
        """)
        yield ASYNC_NETWORK_TABLE
        await async_postgres_backend.execute(f"DROP TABLE IF EXISTS {ASYNC_NETWORK_TABLE}")

    @pytest.mark.asyncio
    async def test_async_inet_round_trip(
        self, async_postgres_backend, async_network_test_table
    ):
        """Insert an inet value asynchronously and verify PostgreSQL returns it."""
        literal = _inet_literal(ipaddress.ip_address("2001:db8::1"))

        await async_postgres_backend.execute(
            f"INSERT INTO {async_network_test_table} (inet_value) VALUES ({literal})"
        )
        result = await async_postgres_backend.fetch_one(
            f"SELECT host(inet_value) AS inet_value FROM {async_network_test_table}"
        )

        assert result["inet_value"] == "2001:db8::1"

    @pytest.mark.asyncio
    async def test_async_macaddr_round_trip(
        self, async_postgres_backend, async_network_test_table
    ):
        """Insert a macaddr value asynchronously and verify PostgreSQL returns it."""
        literal = _macaddr_literal(PostgresMacaddr("08:00:2b:01:02:03"))

        await async_postgres_backend.execute(
            f"INSERT INTO {async_network_test_table} (mac_value) VALUES ({literal})"
        )
        result = await async_postgres_backend.fetch_one(
            f"SELECT mac_value::text AS mac_value FROM {async_network_test_table}"
        )

        assert result["mac_value"] == "08:00:2b:01:02:03"

    @pytest.mark.asyncio
    async def test_async_network_null_round_trip(
        self, async_postgres_backend, async_network_test_table
    ):
        """Insert NULL network values asynchronously and verify fetched values are None."""
        await async_postgres_backend.execute(f"""
            INSERT INTO {async_network_test_table}
                (inet_value, cidr_value, mac_value)
            VALUES (NULL, NULL, NULL)
        """)
        result = await async_postgres_backend.fetch_one(f"""
            SELECT inet_value, cidr_value, mac_value
            FROM {async_network_test_table}
        """)

        assert result["inet_value"] is None
        assert result["cidr_value"] is None
        assert result["mac_value"] is None
