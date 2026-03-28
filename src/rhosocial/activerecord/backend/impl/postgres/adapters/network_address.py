# src/rhosocial/activerecord/backend/impl/postgres/adapters/network_address.py
"""
PostgreSQL Network Address Types Adapters.

This module provides type adapters for PostgreSQL network address types.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/datatype-net-types.html

Network Address Types:
- inet: IPv4 or IPv6 host address, optionally with subnet mask
- cidr: IPv4 or IPv6 network address
- macaddr: MAC address (6-byte, EUI-48 format)
- macaddr8: MAC address (8-byte, EUI-64 format, PostgreSQL 10+)
"""

from typing import Any, Dict, List, Optional, Set, Type

from ..types.network_address import PostgresMacaddr, PostgresMacaddr8


class PostgresNetworkAddressAdapter:
    """
    Adapts Python ipaddress objects to PostgreSQL network types.

    This adapter handles conversion between Python ipaddress module
    objects and PostgreSQL inet/cidr types.

    Supported Python types:
    - ipaddress.IPv4Address
    - ipaddress.IPv6Address
    - ipaddress.IPv4Network
    - ipaddress.IPv6Network
    """

    @property
    def supported_types(self) -> Dict[Type, List[Any]]:
        try:
            import ipaddress

            return {
                ipaddress.IPv4Address: [str],
                ipaddress.IPv6Address: [str],
                ipaddress.IPv4Network: [str],
                ipaddress.IPv6Network: [str],
            }
        except ImportError:
            return {}

    def to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Any:
        if value is None:
            return None
        return str(value)

    def from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Any:
        if value is None:
            return None
        try:
            import ipaddress

            return ipaddress.ip_address(value)
        except (ImportError, ValueError):
            try:
                import ipaddress

                return ipaddress.ip_network(value)
            except (ImportError, ValueError):
                return value

    def to_database_batch(
        self, values: List[Any], target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(
        self, values: List[Any], target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        return [self.from_database(v, target_type, options) for v in values]


class PostgresMacaddrAdapter:
    """PostgreSQL MACADDR type adapter.

    This adapter converts between Python PostgresMacaddr/str and
    PostgreSQL MACADDR values.
    """

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        """Return supported type mappings."""
        return {
            PostgresMacaddr: {str},
        }

    def to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Convert Python value to PostgreSQL MACADDR.

        Args:
            value: Python value (PostgresMacaddr, str, or bytes)
            target_type: Target type (str)
            options: Optional conversion options

        Returns:
            MAC address string in PostgreSQL format, or None

        Raises:
            TypeError: If value type is not supported
            ValueError: If MAC address format is invalid
        """
        if value is None:
            return None

        if isinstance(value, PostgresMacaddr):
            return str(value)

        if isinstance(value, str):
            mac = PostgresMacaddr(value)
            return str(mac)

        if isinstance(value, bytes):
            mac = PostgresMacaddr(value)
            return str(mac)

        raise TypeError(f"Cannot convert {type(value).__name__} to MACADDR")

    def from_database(
        self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> Optional[PostgresMacaddr]:
        """Convert PostgreSQL MACADDR to Python object.

        Args:
            value: Database value (string)
            target_type: Target type (PostgresMacaddr)
            options: Optional conversion options

        Returns:
            PostgresMacaddr instance, or None

        Raises:
            TypeError: If value type is not supported
            ValueError: If MAC address format is invalid
        """
        if value is None:
            return None

        if isinstance(value, PostgresMacaddr):
            return value

        if isinstance(value, str):
            return PostgresMacaddr(value)

        raise TypeError(f"Cannot convert {type(value).__name__} from MACADDR")

    def to_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        """Batch convert Python values to PostgreSQL format."""
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        """Batch convert database values to Python objects."""
        return [self.from_database(v, target_type, options) for v in values]


class PostgresMacaddr8Adapter:
    """PostgreSQL MACADDR8 type adapter.

    This adapter converts between Python PostgresMacaddr8/str and
    PostgreSQL MACADDR8 values.

    Note: MACADDR8 type is available since PostgreSQL 10.
    """

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        """Return supported type mappings."""
        return {
            PostgresMacaddr8: {str},
        }

    def to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Convert Python value to PostgreSQL MACADDR8.

        Args:
            value: Python value (PostgresMacaddr8, str, or bytes)
            target_type: Target type (str)
            options: Optional conversion options

        Returns:
            MAC address string in PostgreSQL format, or None

        Raises:
            TypeError: If value type is not supported
            ValueError: If MAC address format is invalid
        """
        if value is None:
            return None

        if isinstance(value, PostgresMacaddr8):
            return str(value)

        if isinstance(value, str):
            mac = PostgresMacaddr8(value)
            return str(mac)

        if isinstance(value, bytes):
            mac = PostgresMacaddr8(value)
            return str(mac)

        raise TypeError(f"Cannot convert {type(value).__name__} to MACADDR8")

    def from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Any:
        """Convert PostgreSQL MACADDR8 to Python object.

        Args:
            value: Database value (string)
            target_type: Target type (PostgresMacaddr8)
            options: Optional conversion options

        Returns:
            PostgresMacaddr8 instance, or None

        Raises:
            TypeError: If value type is not supported
            ValueError: If MAC address format is invalid
        """
        if value is None:
            return None

        if isinstance(value, PostgresMacaddr8):
            return value

        if isinstance(value, str):
            return PostgresMacaddr8(value)

        raise TypeError(f"Cannot convert {type(value).__name__} from MACADDR8")

    def to_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        """Batch convert Python values to PostgreSQL format."""
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(self, values: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        """Batch convert database values to Python objects."""
        return [self.from_database(v, target_type, options) for v in values]


__all__ = [
    "PostgresNetworkAddressAdapter",
    "PostgresMacaddrAdapter",
    "PostgresMacaddr8Adapter",
]
