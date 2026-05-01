# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/orafce.py
"""orafce extension protocol definition.

This module defines the protocol for orafce Oracle compatibility
functionality in PostgreSQL.

For SQL expression generation, use the function factories in
``functions/orafce.py`` instead of the removed format_* methods.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresOrafceSupport(Protocol):
    """orafce Oracle compatibility extension protocol.

    Feature Source: Extension support (requires orafce extension)

    orafce provides Oracle compatibility functions:
    - String functions
    - Date functions
    - Type conversions

    Extension Information:
    - Extension name: orafce
    - Install command: CREATE EXTENSION orafce;
    - Minimum version: 3.0
    - Documentation: https://github.com/orafce/orafce
    """

    def supports_orafce(self) -> bool:
        """Whether orafce extension is available."""
        ...
