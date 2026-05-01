# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/earthdistance.py
"""PostgreSQL earthdistance extension protocol.

This module defines the protocol for earthdistance extension support,
which provides earth distance calculations for geographic points.

For SQL expression generation, use the function factories in
``functions/earthdistance.py`` instead of the removed format_* methods.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresEarthdistanceSupport(Protocol):
    """earthdistance earth distance calculation protocol.

    Feature Source: Extension support (requires earthdistance extension)

    earthdistance provides earth distance calculations:
    - earth data type for surface points
    - Great-circle distance calculation
    - Distance operators (<@> for miles)

    Extension Information:
    - Extension name: earthdistance
    - Install command: CREATE EXTENSION earthdistance;
    - Minimum version: 1.0
    - Dependencies: cube extension
    - Documentation: https://www.postgresql.org/docs/current/earthdistance.html
    """

    def supports_earthdistance_type(self) -> bool:
        """Whether earth data type is supported."""
        ...

    def supports_earthdistance_operators(self) -> bool:
        """Whether earthdistance operators are supported."""
        ...
