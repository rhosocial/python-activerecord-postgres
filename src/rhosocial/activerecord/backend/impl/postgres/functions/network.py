# src/rhosocial/activerecord/backend/impl/postgres/functions/network.py
"""
PostgreSQL Network Address Functions.

This module provides SQL expression generators for PostgreSQL network
address functions and operators.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-net.html

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return SQL expression strings
"""

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _to_sql(expr: Any) -> str:
    """Convert an expression to its SQL string representation."""
    if hasattr(expr, 'to_sql'):
        return expr.to_sql()[0]
    return str(expr)


def inet_client_addr(dialect: "SQLDialectBase") -> str:
    """
    Return the client's IP address from the current session.

    Returns:
        SQL expression: inet_client_addr()

    Example:
        >>> inet_client_addr(dialect)
        'inet_client_addr()'
    """
    return "inet_client_addr()"


def inet_client_port(dialect: "SQLDialectBase") -> str:
    """
    Return the client's port number from the current session.

    Returns:
        SQL expression: inet_client_port()

    Example:
        >>> inet_client_port(dialect)
        'inet_client_port()'
    """
    return "inet_client_port()"


def inet_server_addr(dialect: "SQLDialectBase") -> str:
    """
    Return the server's IP address for the current connection.

    Returns:
        SQL expression: inet_server_addr()

    Example:
        >>> inet_server_addr(dialect)
        'inet_server_addr()'
    """
    return "inet_server_addr()"


def inet_server_port(dialect: "SQLDialectBase") -> str:
    """
    Return the server's port number for the current connection.

    Returns:
        SQL expression: inet_server_port()

    Example:
        >>> inet_server_port(dialect)
        'inet_server_port()'
    """
    return "inet_server_port()"


def inet_merge(dialect: "SQLDialectBase", net1: Any, net2: Any) -> str:
    """
    Compute the smallest network that includes both input networks.

    Args:
        dialect: The SQL dialect instance
        net1: First network (inet or cidr)
        net2: Second network (inet or cidr)

    Returns:
        SQL expression: inet_merge(net1, net2)

    Example:
        >>> inet_merge(dialect, "'192.168.1.0/24'", "'192.168.2.0/24'")
        "inet_merge('192.168.1.0/24', '192.168.2.0/24')"
    """
    return f"inet_merge({_to_sql(net1)}, {_to_sql(net2)})"


def inet_and(dialect: "SQLDialectBase", addr1: Any, addr2: Any) -> str:
    """
    Compute bitwise AND of two IP addresses.

    Args:
        dialect: The SQL dialect instance
        addr1: First IP address (inet or cidr)
        addr2: Second IP address (inet or cidr)

    Returns:
        SQL expression: addr1 & addr2

    Example:
        >>> inet_and(dialect, "'192.168.1.1'", "'255.255.255.0'")
        "inet_and('192.168.1.1', '255.255.255.0')"
    """
    return f"inet_and({_to_sql(addr1)}, {_to_sql(addr2)})"


def inet_or(dialect: "SQLDialectBase", addr1: Any, addr2: Any) -> str:
    """
    Compute bitwise OR of two IP addresses.

    Args:
        dialect: The SQL dialect instance
        addr1: First IP address (inet or cidr)
        addr2: Second IP address (inet or cidr)

    Returns:
        SQL expression: addr1 | addr2

    Example:
        >>> inet_or(dialect, "'192.168.1.1'", "'0.0.0.255'")
        "inet_or('192.168.1.1', '0.0.0.255')"
    """
    return f"inet_or({_to_sql(addr1)}, {_to_sql(addr2)})"


def inetnot(dialect: "SQLDialectBase", addr: Any) -> str:
    """
    Compute bitwise NOT of an IP address.

    Args:
        dialect: The SQL dialect instance
        addr: IP address (inet or cidr)

    Returns:
        SQL expression: ~addr

    Example:
        >>> inetnot(dialect, "'192.168.1.1'")
        "inetnot('192.168.1.1')"
    """
    return f"inetnot({_to_sql(addr)})"


def inet_set_mask(dialect: "SQLDialectBase", addr: Any, mask_len: Any) -> str:
    """
    Set the mask length for an IP address.

    Args:
        dialect: The SQL dialect instance
        addr: IP address (inet or cidr)
        mask_len: The mask length

    Returns:
        SQL expression: set_masklen(addr, mask_len)

    Example:
        >>> inet_set_mask(dialect, "'192.168.1.1'", 24)
        "set_masklen('192.168.1.1', 24)"
    """
    return f"set_masklen({_to_sql(addr)}, {_to_sql(mask_len)})"


def inet_masklen(dialect: "SQLDialectBase", addr: Any) -> str:
    """
    Return the mask length of an IP address.

    Args:
        dialect: The SQL dialect instance
        addr: IP address (inet or cidr)

    Returns:
        SQL expression: masklen(addr)

    Example:
        >>> inet_masklen(dialect, "'192.168.1.1/24'")
        "masklen('192.168.1.1/24')"
    """
    return f"masklen({_to_sql(addr)})"


def inet_netmask(dialect: "SQLDialectBase", addr: Any) -> str:
    """
    Return the network mask of an IP address.

    Args:
        dialect: The SQL dialect instance
        addr: IP address (inet or cidr)

    Returns:
        SQL expression: netmask(addr)

    Example:
        >>> inet_netmask(dialect, "'192.168.1.1/24'")
        "netmask('192.168.1.1/24')"
    """
    return f"netmask({_to_sql(addr)})"


def inet_network(dialect: "SQLDialectBase", addr: Any) -> str:
    """
    Extract the network part of an IP address.

    Args:
        dialect: The SQL dialect instance
        addr: IP address (inet or cidr)

    Returns:
        SQL expression: network(addr)

    Example:
        >>> inet_network(dialect, "'192.168.1.1/24'")
        "network('192.168.1.1/24')"
    """
    return f"network({_to_sql(addr)})"


def inet_recv(dialect: "SQLDialectBase", addr: Any) -> str:
    """
    Extract the host part of an IP address.

    Args:
        dialect: The SQL dialect instance
        addr: IP address (inet or cidr)

    Returns:
        SQL expression: host(addr)

    Example:
        >>> inet_recv(dialect, "'192.168.1.1/24'")
        "host('192.168.1.1/24')"
    """
    return f"host({_to_sql(addr)})"


def inet_show(dialect: "SQLDialectBase", addr: Any) -> str:
    """
    Return the IP address as text.

    Args:
        dialect: The SQL dialect instance
        addr: IP address (inet or cidr)

    Returns:
        SQL expression: text(addr)

    Example:
        >>> inet_show(dialect, "'192.168.1.1'")
        "text('192.168.1.1')"
    """
    return f"text({_to_sql(addr)})"


def cidr_netmask(dialect: "SQLDialectBase", addr: Any) -> str:
    """
    Return the broadcast address of the network.

    Args:
        dialect: The SQL dialect instance
        addr: CIDR address

    Returns:
        SQL expression: broadcast(addr)

    Example:
        >>> cidr_netmask(dialect, "'192.168.1.0/24'")
        "broadcast('192.168.1.0/24')"
    """
    return f"broadcast({_to_sql(addr)})"


def macaddr8_set7bit(dialect: "SQLDialectBase", mac: Any) -> str:
    """
    Set the 7th bit of the MAC address (marking it as a multicast address).

    Args:
        dialect: The SQL dialect instance
        mac: MAC address

    Returns:
        SQL expression: macaddr8_set7bit(mac)

    Example:
        >>> macaddr8_set7bit(dialect, "'08:00:2b:01:02:03'")
        "macaddr8_set7bit('08:00:2b:01:02:03')"
    """
    return f"macaddr8_set7bit({_to_sql(mac)})"


__all__ = [
    "inet_client_addr",
    "inet_client_port",
    "inet_server_addr",
    "inet_server_port",
    "inet_merge",
    "inet_and",
    "inet_or",
    "inetnot",
    "inet_set_mask",
    "inet_masklen",
    "inet_netmask",
    "inet_network",
    "inet_recv",
    "inet_show",
    "cidr_netmask",
    "macaddr8_set7bit",
]