# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/cube.py
"""cube extension protocol definition.

This module defines the protocol for cube multidimensional cube
functionality in PostgreSQL.

For SQL expression generation, use the function factories in
``functions/cube.py`` instead of the removed format_* methods.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresCubeSupport(Protocol):
    """cube multidimensional cube extension protocol.

    Feature Source: Extension support (requires cube extension)

    cube provides multidimensional cube data type:
    - Point representation
    - Cube operations

    Extension Information:
    - Extension name: cube
    - Install command: CREATE EXTENSION cube;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/cube.html
    """

    def supports_cube_type(self) -> bool:
        """Whether cube type is supported."""
        ...