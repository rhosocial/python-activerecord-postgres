# src/rhosocial/activerecord/backend/impl/postgres/types/network_address.py
"""PostgreSQL Network Address Types.

This module provides type representations for PostgreSQL network address types:
- PostgresMacaddr: MAC address (6-byte, EUI-48 format)
- PostgresMacaddr8: MAC address (8-byte, EUI-64 format, PostgreSQL 10+)

PostgreSQL Documentation: https://www.postgresql.org/docs/current/datatype-net-types.html

MACADDR (PostgresMacaddr):
- Stores MAC addresses in EUI-48 format (6 bytes)
- Input formats: '08:00:2b:01:02:03', '08-00-2b-01-02-03', '08002b:010203', '08002b010203'

MACADDR8 (PostgresMacaddr8):
- Stores MAC addresses in EUI-64 format (8 bytes)
- Available since PostgreSQL 10
- Can accept 6-byte addresses and convert to 8-byte by inserting FF:FE

For type adapters (conversion between Python and database),
see adapters.network_address module.
"""
from dataclasses import dataclass
from typing import Union
import re


@dataclass
class PostgresMacaddr:
    """PostgreSQL MACADDR type representation.

    The MACADDR type stores MAC addresses in EUI-48 format (6 bytes).

    Attributes:
        address: MAC address as string or bytes

    Examples:
        PostgresMacaddr('08:00:2b:01:02:03')
        PostgresMacaddr('08-00-2b-01-02-03')
        PostgresMacaddr('08002b:010203')
        PostgresMacaddr('08002b010203')
        PostgresMacaddr(b'\\x08\\x00\\x2b\\x01\\x02\\x03')
    """
    address: Union[str, bytes]

    def __post_init__(self):
        """Validate and normalize MAC address."""
        if isinstance(self.address, bytes):
            self.address = self._bytes_to_str(self.address)
        self.address = self._normalize(self.address)

    def _bytes_to_str(self, data: bytes) -> str:
        """Convert bytes to MAC address string."""
        hex_str = data.hex()
        return ':'.join(hex_str[i:i+2] for i in range(0, len(hex_str), 2))

    def _normalize(self, addr: str) -> str:
        """Normalize MAC address to canonical format (lowercase, colons)."""
        addr = addr.strip().lower()
        parts = self._parse(addr)
        return ':'.join(f'{p:02x}' for p in parts)

    def _parse(self, addr: str) -> list:
        """Parse MAC address string to list of integers."""
        addr = addr.strip().lower()
        addr = re.sub(r'[:\-\.]', '', addr)

        if len(addr) != 12:
            raise ValueError(
                f"Invalid MAC address length: {len(addr)} hex digits "
                f"(expected 12 for EUI-48)"
            )

        try:
            return [int(addr[i:i+2], 16) for i in range(0, 12, 2)]
        except ValueError as e:
            raise ValueError(f"Invalid hex digit in MAC address: {addr}") from e

    def __str__(self) -> str:
        """Return MAC address in canonical format."""
        return str(self.address)

    def __repr__(self) -> str:
        return f"PostgresMacaddr('{self.address}')"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, PostgresMacaddr):
            return self.address == other.address
        if isinstance(other, str):
            try:
                other_normalized = PostgresMacaddr(other).address
                return self.address == other_normalized
            except ValueError:
                return False
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.address)

    def __bytes__(self) -> bytes:
        """Return MAC address as bytes."""
        parts = str(self.address).split(':')
        return bytes(int(p, 16) for p in parts)


@dataclass
class PostgresMacaddr8:
    """PostgreSQL MACADDR8 type representation.

    The MACADDR8 type stores MAC addresses in EUI-64 format (8 bytes).
    It can also accept EUI-48 format (6 bytes) and convert to EUI-64.

    Attributes:
        address: MAC address as string or bytes

    Examples:
        PostgresMacaddr8('08:00:2b:01:02:03:04:05')
        PostgresMacaddr8('08:00:2b:01:02:03')  # 6-byte, converted to 8-byte
        PostgresMacaddr8(b'\\x08\\x00\\x2b\\x01\\x02\\x03\\x04\\x05')  # bytes format
    """
    address: Union[str, bytes]

    def __post_init__(self):
        """Validate and normalize MAC address."""
        if isinstance(self.address, bytes):
            self.address = self._bytes_to_str(self.address)
        self.address = self._normalize(self.address)

    def _bytes_to_str(self, data: bytes) -> str:
        """Convert bytes to MAC address string."""
        hex_str = data.hex()
        if len(hex_str) == 12:
            hex_str = hex_str[:6] + 'fffe' + hex_str[6:]
        return ':'.join(hex_str[i:i+2] for i in range(0, len(hex_str), 2))

    def _normalize(self, addr: str) -> str:
        """Normalize MAC address to canonical format (lowercase, colons)."""
        addr = addr.strip().lower()
        parts = self._parse(addr)
        return ':'.join(f'{p:02x}' for p in parts)

    def _parse(self, addr: str) -> list:
        """Parse MAC address string to list of integers."""
        addr = addr.strip().lower()
        addr = re.sub(r'[:\-.]', '', addr)

        if len(addr) == 12:
            addr = addr[:6] + 'fffe' + addr[6:]
        elif len(addr) == 16:
            pass
        else:
            raise ValueError(
                f"Invalid MAC address length: {len(addr)} hex digits "
                f"(expected 12 or 16)"
            )

        try:
            return [int(addr[i:i+2], 16) for i in range(0, 16, 2)]
        except ValueError as e:
            raise ValueError(f"Invalid hex digit in MAC address: {addr}") from e

    def __str__(self) -> str:
        """Return MAC address in canonical format."""
        return str(self.address)

    def __repr__(self) -> str:
        return f"PostgresMacaddr8('{self.address}')"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, PostgresMacaddr8):
            return self.address == other.address
        if isinstance(other, str):
            try:
                other_normalized = PostgresMacaddr8(other).address
                return self.address == other_normalized
            except ValueError:
                return False
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.address)

    def __bytes__(self) -> bytes:
        """Return MAC address as bytes."""
        parts = str(self.address).split(':')
        return bytes(int(p, 16) for p in parts)

    @property
    def is_eui48_derived(self) -> bool:
        """Check if this address was derived from EUI-48.

        EUI-48 derived addresses have FF:FE in bytes 4 and 5.
        """
        parts = str(self.address).split(':')
        return parts[3] == 'ff' and parts[4] == 'fe'

    def to_ipv6_interface(self, prefix: str = 'fe80::') -> str:
        """Convert to IPv6 interface identifier.

        For use in IPv6 stateless address autoconfiguration.
        Note: This does NOT set the 7th bit (universal/local flag).
        Use macaddr8_set7bit() for that.

        Args:
            prefix: IPv6 prefix (default: fe80:: for link-local)

        Returns:
            IPv6 address string with MAC as interface ID
        """
        parts = str(self.address).split(':')
        interface_id = ':'.join([
            f'{int(parts[0], 16):02x}{int(parts[1], 16):02x}',
            f'{int(parts[2], 16):02x}{int(parts[3], 16):02x}',
            f'{int(parts[4], 16):02x}{int(parts[5], 16):02x}',
            f'{int(parts[6], 16):02x}{int(parts[7], 16):02x}',
        ])
        return f'{prefix}{interface_id}'

    def set7bit(self) -> 'PostgresMacaddr8':
        """Set the 7th bit (Universal/Local flag) for IPv6 use.

        This modifies the first byte to set the U/L bit,
        as required for modified EUI-64 format in IPv6.

        Returns:
            New PostgresMacaddr8 with 7th bit set
        """
        parts = str(self.address).split(':')
        first_byte = int(parts[0], 16) | 0x02
        new_parts = [f'{first_byte:02x}'] + parts[1:]
        return PostgresMacaddr8(':'.join(new_parts))


__all__ = ['PostgresMacaddr', 'PostgresMacaddr8']
