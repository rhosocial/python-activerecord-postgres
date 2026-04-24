# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/uuid_ossp.py
"""uuid-ossp extension protocol definition.

This module defines the protocol for uuid-ossp UUID generation
functionality in PostgreSQL.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresUuidOssSupport(Protocol):
    """uuid-ossp UUID generation extension protocol.

    Feature Source: Extension support (requires uuid-ossp extension)

    uuid-ossp provides UUID generation functions:
    - Generate UUIDs using various algorithms
    - Version 1 (MAC address + time)
    - Version 4 (random)

    Extension Information:
    - Extension name: uuid-ossp
    - Install command: CREATE EXTENSION uuid-ossp;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/uuid-ossp.html
    """

    def supports_uuid_generation(self) -> bool:
        """Whether UUID generation functions are supported."""
        ...

    def format_uuid_generate_v1(self) -> str:
        """Format UUID v1 generation."""
        ...

    def format_uuid_generate_v1mc(self) -> str:
        """Format UUID v1mc generation."""
        ...

    def format_uuid_generate_v4(self) -> str:
        """Format UUID v4 generation."""
        ...

    def format_uuid_generate_v5(self, namespace: str, name: str) -> str:
        """Format UUID v5 generation."""
        ...