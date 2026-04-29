# src/rhosocial/activerecord/backend/impl/postgres/functions/network.py
"""
PostgreSQL Network Address Functions and Operators.

This module provides SQL expression generators for PostgreSQL network
address functions and operators. All functions return Expression objects
that integrate with the Expression/Dialect architecture.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-net.html

Session information functions (no arguments):
- inet_client_addr(): Client's IP address
- inet_client_port(): Client's port number
- inet_server_addr(): Server's IP address
- inet_server_port(): Server's port number

Network functions:
- inet_merge(a, b): Smallest network including both inputs
- inet_set_mask(a, mask): Set mask length for an IP address
- inet_masklen(a): Extract mask length
- inet_netmask(a): Construct network mask
- inet_network(a): Extract network part of address
- inet_recv(a): Internal: receive from binary string
- inet_show(a): Display IP address as text
- cidr_netmask(a): Construct cidr network mask
- macaddr8_set7bit(mac): Set 7th bit of MAC address

Network operators:
- inet_and(a, b): Bitwise AND (a & b)
- inet_or(a, b): Bitwise OR (a | b)
- inetnot(a): Bitwise NOT (~a)
"""

from typing import Any, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core
from rhosocial.activerecord.backend.expression.operators import BinaryExpression, UnaryExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression.

    Supports strings and existing BaseExpression objects.

    Args:
        dialect: The SQL dialect instance
        expr: Value to convert

    Returns:
        BaseExpression representing the value
    """
    if isinstance(expr, bases.BaseExpression):
        return expr
    elif isinstance(expr, str):
        return core.Literal(dialect, expr)
    else:
        return core.Literal(dialect, expr)


# ============== Session Information Functions ==============

def inet_client_addr(dialect: "SQLDialectBase") -> core.FunctionCall:
    """
    Return the client's IP address from the current session.

    Args:
        dialect: The SQL dialect instance

    Returns:
        FunctionCall: SQL expression for inet_client_addr()

    Example:
        >>> func = inet_client_addr(dialect)
        >>> func.to_sql()
        ('inet_client_addr()', ())
    """
    return core.FunctionCall(dialect, "inet_client_addr")


def inet_client_port(dialect: "SQLDialectBase") -> core.FunctionCall:
    """
    Return the client's port number from the current session.

    Args:
        dialect: The SQL dialect instance

    Returns:
        FunctionCall: SQL expression for inet_client_port()

    Example:
        >>> func = inet_client_port(dialect)
        >>> func.to_sql()
        ('inet_client_port()', ())
    """
    return core.FunctionCall(dialect, "inet_client_port")


def inet_server_addr(dialect: "SQLDialectBase") -> core.FunctionCall:
    """
    Return the server's IP address for the current connection.

    Args:
        dialect: The SQL dialect instance

    Returns:
        FunctionCall: SQL expression for inet_server_addr()

    Example:
        >>> func = inet_server_addr(dialect)
        >>> func.to_sql()
        ('inet_server_addr()', ())
    """
    return core.FunctionCall(dialect, "inet_server_addr")


def inet_server_port(dialect: "SQLDialectBase") -> core.FunctionCall:
    """
    Return the server's port number for the current connection.

    Args:
        dialect: The SQL dialect instance

    Returns:
        FunctionCall: SQL expression for inet_server_port()

    Example:
        >>> func = inet_server_port(dialect)
        >>> func.to_sql()
        ('inet_server_port()', ())
    """
    return core.FunctionCall(dialect, "inet_server_port")


# ============== Network Functions ==============

def inet_merge(
    dialect: "SQLDialectBase",
    net1: Any,
    net2: Any,
) -> core.FunctionCall:
    """
    Compute the smallest network that includes both input networks.

    Args:
        dialect: The SQL dialect instance
        net1: First network (inet or cidr)
        net2: Second network (inet or cidr)

    Returns:
        FunctionCall: SQL expression for inet_merge(net1, net2)

    Example:
        >>> func = inet_merge(dialect, "'192.168.1.0/24'", "'192.168.2.0/24'")
        >>> func.to_sql()
        ('inet_merge(%s, %s)', ('192.168.1.0/24', '192.168.2.0/24'))
    """
    return core.FunctionCall(
        dialect, "inet_merge",
        _convert_to_expression(dialect, net1),
        _convert_to_expression(dialect, net2),
    )


# ============== Network Operators ==============

def inet_and(
    dialect: "SQLDialectBase",
    addr1: Any,
    addr2: Any,
) -> BinaryExpression:
    """
    Compute bitwise AND of two IP addresses.

    Args:
        dialect: The SQL dialect instance
        addr1: First IP address (inet or cidr)
        addr2: Second IP address (inet or cidr)

    Returns:
        BinaryExpression: SQL expression for addr1 & addr2

    Example:
        >>> expr = inet_and(dialect, "'192.168.1.1'", "'255.255.255.0'")
        >>> expr.to_sql()
        ('(%s & %s)', ('192.168.1.1', '255.255.255.0'))
    """
    return BinaryExpression(
        dialect, "&",
        _convert_to_expression(dialect, addr1),
        _convert_to_expression(dialect, addr2),
    )


def inet_or(
    dialect: "SQLDialectBase",
    addr1: Any,
    addr2: Any,
) -> BinaryExpression:
    """
    Compute bitwise OR of two IP addresses.

    Args:
        dialect: The SQL dialect instance
        addr1: First IP address (inet or cidr)
        addr2: Second IP address (inet or cidr)

    Returns:
        BinaryExpression: SQL expression for addr1 | addr2

    Example:
        >>> expr = inet_or(dialect, "'192.168.1.1'", "'0.0.0.255'")
        >>> expr.to_sql()
        ('(%s | %s)', ('192.168.1.1', '0.0.0.255'))
    """
    return BinaryExpression(
        dialect, "|",
        _convert_to_expression(dialect, addr1),
        _convert_to_expression(dialect, addr2),
    )


def inetnot(
    dialect: "SQLDialectBase",
    addr: Any,
) -> UnaryExpression:
    """
    Compute bitwise NOT of an IP address.

    Args:
        dialect: The SQL dialect instance
        addr: IP address (inet or cidr)

    Returns:
        UnaryExpression: SQL expression for ~addr

    Example:
        >>> expr = inetnot(dialect, "'192.168.1.1'")
        >>> expr.to_sql()
        ('(~%s)', ('192.168.1.1',))
    """
    return UnaryExpression(
        dialect, "~",
        _convert_to_expression(dialect, addr),
        pos="before",
    )


# ============== Network Address Functions ==============

def inet_set_mask(
    dialect: "SQLDialectBase",
    addr: Any,
    mask_len: Any,
) -> core.FunctionCall:
    """
    Set the mask length for an IP address.

    Args:
        dialect: The SQL dialect instance
        addr: IP address (inet or cidr)
        mask_len: The mask length

    Returns:
        FunctionCall: SQL expression for set_masklen(addr, mask_len)

    Example:
        >>> func = inet_set_mask(dialect, "'192.168.1.1'", 24)
        >>> func.to_sql()
        ('set_masklen(%s, %s)', ('192.168.1.1', 24))
    """
    return core.FunctionCall(
        dialect, "set_masklen",
        _convert_to_expression(dialect, addr),
        _convert_to_expression(dialect, mask_len),
    )


def inet_masklen(
    dialect: "SQLDialectBase",
    addr: Any,
) -> core.FunctionCall:
    """
    Return the mask length of an IP address.

    Args:
        dialect: The SQL dialect instance
        addr: IP address (inet or cidr)

    Returns:
        FunctionCall: SQL expression for masklen(addr)

    Example:
        >>> func = inet_masklen(dialect, "'192.168.1.1/24'")
        >>> func.to_sql()
        ('masklen(%s)', ('192.168.1.1/24',))
    """
    return core.FunctionCall(dialect, "masklen", _convert_to_expression(dialect, addr))


def inet_netmask(
    dialect: "SQLDialectBase",
    addr: Any,
) -> core.FunctionCall:
    """
    Return the network mask of an IP address.

    Args:
        dialect: The SQL dialect instance
        addr: IP address (inet or cidr)

    Returns:
        FunctionCall: SQL expression for netmask(addr)

    Example:
        >>> func = inet_netmask(dialect, "'192.168.1.1/24'")
        >>> func.to_sql()
        ('netmask(%s)', ('192.168.1.1/24',))
    """
    return core.FunctionCall(dialect, "netmask", _convert_to_expression(dialect, addr))


def inet_network(
    dialect: "SQLDialectBase",
    addr: Any,
) -> core.FunctionCall:
    """
    Extract the network part of an IP address.

    Args:
        dialect: The SQL dialect instance
        addr: IP address (inet or cidr)

    Returns:
        FunctionCall: SQL expression for network(addr)

    Example:
        >>> func = inet_network(dialect, "'192.168.1.1/24'")
        >>> func.to_sql()
        ('network(%s)', ('192.168.1.1/24',))
    """
    return core.FunctionCall(dialect, "network", _convert_to_expression(dialect, addr))


def inet_recv(
    dialect: "SQLDialectBase",
    addr: Any,
) -> core.FunctionCall:
    """
    Internal function: receive an IP address from a binary string.

    Args:
        dialect: The SQL dialect instance
        addr: Binary string value

    Returns:
        FunctionCall: SQL expression for recv(addr)

    Example:
        >>> func = inet_recv(dialect, "binary_value")
        >>> func.to_sql()
        ('recv(%s)', ('binary_value',))
    """
    return core.FunctionCall(dialect, "recv", _convert_to_expression(dialect, addr))


def inet_show(
    dialect: "SQLDialectBase",
    addr: Any,
) -> core.FunctionCall:
    """
    Return the IP address as text.

    Args:
        dialect: The SQL dialect instance
        addr: IP address (inet or cidr)

    Returns:
        FunctionCall: SQL expression for text(addr)

    Example:
        >>> func = inet_show(dialect, "'192.168.1.1'")
        >>> func.to_sql()
        ('text(%s)', ('192.168.1.1',))
    """
    return core.FunctionCall(dialect, "text", _convert_to_expression(dialect, addr))


def cidr_netmask(
    dialect: "SQLDialectBase",
    addr: Any,
) -> core.FunctionCall:
    """
    Return the broadcast address of the network.

    Args:
        dialect: The SQL dialect instance
        addr: CIDR address

    Returns:
        FunctionCall: SQL expression for broadcast(addr)

    Example:
        >>> func = cidr_netmask(dialect, "'192.168.1.0/24'")
        >>> func.to_sql()
        ('broadcast(%s)', ('192.168.1.0/24',))
    """
    return core.FunctionCall(dialect, "broadcast", _convert_to_expression(dialect, addr))


def macaddr8_set7bit(
    dialect: "SQLDialectBase",
    mac: Any,
) -> core.FunctionCall:
    """
    Set the 7th bit of the MAC address (marking it as a multicast address).

    Args:
        dialect: The SQL dialect instance
        mac: MAC address

    Returns:
        FunctionCall: SQL expression for macaddr8_set7bit(mac)

    Example:
        >>> func = macaddr8_set7bit(dialect, "'08:00:2b:01:02:03'")
        >>> func.to_sql()
        ('macaddr8_set7bit(%s)', ('08:00:2b:01:02:03',))
    """
    return core.FunctionCall(dialect, "macaddr8_set7bit", _convert_to_expression(dialect, mac))


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
