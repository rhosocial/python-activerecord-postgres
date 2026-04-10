# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_postgres_network_functions.py
"""
Tests for PostgreSQL Network Address functions.

Functions: inet_client_addr, inet_client_port, inet_server_addr,
           inet_server_port, inet_merge, inet_and, inet_or, inetnot,
           inet_set_mask, inet_masklen, inet_netmask, inet_network,
           inet_recv, inet_show, cidr_netmask, macaddr8_set7bit
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions.network import (
    inet_client_addr,
    inet_client_port,
    inet_server_addr,
    inet_server_port,
    inet_merge,
    inet_and,
    inet_or,
    inetnot,
    inet_set_mask,
    inet_masklen,
    inet_netmask,
    inet_network,
    inet_recv,
    inet_show,
    cidr_netmask,
    macaddr8_set7bit,
)


class TestPostgresNetworkFunctions:
    """Tests for PostgreSQL network functions."""

    def test_inet_client_addr(self, postgres_dialect: PostgresDialect):
        """Test inet_client_addr() function."""
        result = inet_client_addr(postgres_dialect)
        assert result == "inet_client_addr()"

    def test_inet_client_port(self, postgres_dialect: PostgresDialect):
        """Test inet_client_port() function."""
        result = inet_client_port(postgres_dialect)
        assert result == "inet_client_port()"

    def test_inet_server_addr(self, postgres_dialect: PostgresDialect):
        """Test inet_server_addr() function."""
        result = inet_server_addr(postgres_dialect)
        assert result == "inet_server_addr()"

    def test_inet_server_port(self, postgres_dialect: PostgresDialect):
        """Test inet_server_port() function."""
        result = inet_server_port(postgres_dialect)
        assert result == "inet_server_port()"

    def test_inet_merge(self, postgres_dialect: PostgresDialect):
        """Test inet_merge() function."""
        result = inet_merge(postgres_dialect, "'192.168.1.0/24'", "'192.168.2.0/24'")
        assert result == "inet_merge('192.168.1.0/24', '192.168.2.0/24')"

    def test_inet_and(self, postgres_dialect: PostgresDialect):
        """Test inet_and() function."""
        result = inet_and(postgres_dialect, "'192.168.1.1'", "'255.255.255.0'")
        assert result == "inet_and('192.168.1.1', '255.255.255.0')"

    def test_inet_or(self, postgres_dialect: PostgresDialect):
        """Test inet_or() function."""
        result = inet_or(postgres_dialect, "'192.168.1.1'", "'0.0.0.255'")
        assert result == "inet_or('192.168.1.1', '0.0.0.255')"

    def test_inetnot(self, postgres_dialect: PostgresDialect):
        """Test inetnot() function."""
        result = inetnot(postgres_dialect, "'192.168.1.1'")
        assert result == "inetnot('192.168.1.1')"

    def test_inet_set_mask(self, postgres_dialect: PostgresDialect):
        """Test inet_set_mask() function."""
        result = inet_set_mask(postgres_dialect, "'192.168.1.1'", 24)
        assert result == "set_masklen('192.168.1.1', 24)"

    def test_inet_masklen(self, postgres_dialect: PostgresDialect):
        """Test inet_masklen() function."""
        result = inet_masklen(postgres_dialect, "'192.168.1.1/24'")
        assert result == "masklen('192.168.1.1/24')"

    def test_inet_netmask(self, postgres_dialect: PostgresDialect):
        """Test inet_netmask() function."""
        result = inet_netmask(postgres_dialect, "'192.168.1.1/24'")
        assert result == "netmask('192.168.1.1/24')"

    def test_inet_network(self, postgres_dialect: PostgresDialect):
        """Test inet_network() function."""
        result = inet_network(postgres_dialect, "'192.168.1.1/24'")
        assert result == "network('192.168.1.1/24')"

    def test_inet_recv(self, postgres_dialect: PostgresDialect):
        """Test inet_recv() function (host)."""
        result = inet_recv(postgres_dialect, "'192.168.1.1/24'")
        assert result == "host('192.168.1.1/24')"

    def test_inet_show(self, postgres_dialect: PostgresDialect):
        """Test inet_show() function (text)."""
        result = inet_show(postgres_dialect, "'192.168.1.1'")
        assert result == "text('192.168.1.1')"

    def test_cidr_netmask(self, postgres_dialect: PostgresDialect):
        """Test cidr_netmask() function (broadcast)."""
        result = cidr_netmask(postgres_dialect, "'192.168.1.0/24'")
        assert result == "broadcast('192.168.1.0/24')"

    def test_macaddr8_set7bit(self, postgres_dialect: PostgresDialect):
        """Test macaddr8_set7bit() function."""
        result = macaddr8_set7bit(postgres_dialect, "'08:00:2b:01:02:03'")
        assert result == "macaddr8_set7bit('08:00:2b:01:02:03')"