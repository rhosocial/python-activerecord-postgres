# tests/rhosocial/activerecord_postgres_test/feature/backend/adapters/native/test_native_network.py
"""Native tests for PostgreSQL network address types.

``inet``/``cidr`` use ``ipaddress`` objects, with ``host()`` and ``col::text``
as the authoritative server expressions.  ``macaddr``/``macaddr8`` must
normalise uppercase, hyphenated and compact input to the lowercase
colon-separated form.
"""

import ipaddress

import pytest

from rhosocial.activerecord.backend.errors import DatabaseError
from rhosocial.activerecord.backend.impl.postgres.adapters.network_address import (
    PostgresMacaddr8Adapter,
    PostgresMacaddrAdapter,
    PostgresNetworkAddressAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.type_values import (
    PostgresMacaddr,
    PostgresMacaddr8,
)
from rhosocial.activerecord_postgres_test.feature.backend.adapters.native.helpers import (
    insert_row,
    insert_sql,
    read_raw,
    read_value,
    update_cell,
)

TABLE = "native_network"

NETWORK = PostgresNetworkAddressAdapter()
MACADDR = PostgresMacaddrAdapter()
MACADDR8 = PostgresMacaddr8Adapter()


class TestNetwork:
    TABLES = (TABLE,)

    def test_inet_address_and_interface_read_back(self, native_backend):
        """S1/S2: host and netmask survive, and ``host()`` strips the mask."""
        backend = native_backend
        values = {
            "inet_value": NETWORK.to_database(ipaddress.ip_address("192.168.1.5"), str),
        }
        row_id = insert_row(backend, TABLE, values, {"inet_value": "inet"})

        assert read_raw(backend, TABLE, "inet_value", row_id) == "192.168.1.5/32"
        assert read_raw(backend, TABLE, "inet_value", row_id, "host(inet_value)") == (
            "192.168.1.5"
        )
        decoded = NETWORK.from_database(
            read_value(backend, TABLE, "inet_value", row_id), str
        )
        assert decoded == ipaddress.ip_address("192.168.1.5")

        update_cell(backend, TABLE, "inet_value", row_id, "192.168.1.5/32", "inet")
        assert read_raw(backend, TABLE, "inet_value", row_id) == "192.168.1.5/32"

        # An interface keeps the host bits and the mask together.
        interface = ipaddress.ip_interface("192.168.1.5/24")
        row_id = insert_row(
            backend,
            TABLE,
            {"inet_value": NETWORK.to_database(interface, str)},
            {"inet_value": "inet"},
        )
        assert read_raw(backend, TABLE, "inet_value", row_id) == "192.168.1.5/24"
        assert read_raw(backend, TABLE, "inet_value", row_id, "host(inet_value)") == (
            "192.168.1.5"
        )
        assert read_raw(backend, TABLE, "inet_value", row_id, "network(inet_value)::text") == (
            "192.168.1.0/24"
        )
        decoded = NETWORK.from_database(
            read_value(backend, TABLE, "inet_value", row_id), str
        )
        assert decoded == interface

    def test_cidr_network_is_returned_as_a_network_object(self, native_backend):
        """S1/S2: cidr binds as text and psycopg returns an IPv4Network."""
        backend = native_backend
        network = ipaddress.ip_network("192.168.1.0/24")
        row_id = insert_row(
            backend,
            TABLE,
            {"cidr_value": NETWORK.to_database(network, str)},
            {"cidr_value": "cidr"},
        )

        assert read_raw(backend, TABLE, "cidr_value", row_id) == "192.168.1.0/24"
        driver_value = read_value(backend, TABLE, "cidr_value", row_id)
        assert isinstance(driver_value, ipaddress.IPv4Network)
        assert NETWORK.from_database(driver_value, str) == network

        update_cell(backend, TABLE, "cidr_value", row_id, "192.168.1.0/24", "cidr")
        assert read_raw(backend, TABLE, "cidr_value", row_id) == "192.168.1.0/24"

    def test_macaddr_normalises_uppercase_and_hyphenated_input(self, native_backend):
        """S1/S2: the adapter and the server agree on the canonical form."""
        backend = native_backend
        value = PostgresMacaddr("08-00-2B-01-02-03")
        assert MACADDR.to_database(value, str) == "08:00:2b:01:02:03"

        row_id = insert_row(
            backend,
            TABLE,
            {"mac_value": MACADDR.to_database(value, str)},
            {"mac_value": "macaddr"},
        )
        raw = read_raw(backend, TABLE, "mac_value", row_id)
        assert raw == "08:00:2b:01:02:03"
        decoded = MACADDR.from_database(raw, PostgresMacaddr)
        assert decoded == value
        assert MACADDR.to_database(decoded, str) == raw

        update_cell(backend, TABLE, "mac_value", row_id, raw, "macaddr")
        assert read_raw(backend, TABLE, "mac_value", row_id) == raw

    def test_macaddr8_expands_a_six_octet_address(self, native_backend):
        """S1/S2: an EUI-48 input becomes the EUI-64 ``ff:fe`` form."""
        backend = native_backend
        value = PostgresMacaddr8("08002B010203")
        assert MACADDR8.to_database(value, str) == "08:00:2b:ff:fe:01:02:03"

        row_id = insert_row(
            backend,
            TABLE,
            {"mac8_value": MACADDR8.to_database(value, str)},
            {"mac8_value": "macaddr8"},
        )
        raw = read_raw(backend, TABLE, "mac8_value", row_id)
        assert raw == "08:00:2b:ff:fe:01:02:03"
        decoded = MACADDR8.from_database(raw, PostgresMacaddr8)
        assert decoded == value
        assert MACADDR8.to_database(decoded, str) == raw

    def test_external_network_literals_are_parsed(self, native_backend):
        """S3: native SQL text is accepted without an adapter write."""
        backend = native_backend
        row_id = insert_sql(
            backend,
            "INSERT INTO {} (inet_value, mac_value) VALUES "
            "('10.0.0.1'::inet, '08002B010203'::macaddr) RETURNING id".format(TABLE),
        )

        assert read_raw(backend, TABLE, "inet_value", row_id) == "10.0.0.1/32"
        assert read_raw(backend, TABLE, "mac_value", row_id) == "08:00:2b:01:02:03"
        # Plain text carries the /32 mask, so the adapter's fallback decodes a
        # /32 network rather than a bare host; the driver path above returns
        # IPv4Address because psycopg already narrowed it.  This is the
        # documented limitation of parsing external inet text without a hint.
        external = NETWORK.from_database("10.0.0.1/32", str)
        assert isinstance(external, ipaddress.IPv4Network)
        assert str(external) == "10.0.0.1/32"
        assert MACADDR.from_database(
            "08:00:2b:01:02:03", PostgresMacaddr
        ) == PostgresMacaddr("08:00:2b:01:02:03")

    def test_invalid_values_are_rejected(self, native_backend):
        """Invalid input fails before SQL or at the server, never silently."""
        with pytest.raises(ValueError):
            PostgresMacaddr("08:00:2b:01:02")
        with pytest.raises(ValueError):
            PostgresMacaddr("08:00:2b:01:02:zz")
        with pytest.raises(DatabaseError):
            native_backend.execute("SELECT %s::inet", ("not-an-address",))
