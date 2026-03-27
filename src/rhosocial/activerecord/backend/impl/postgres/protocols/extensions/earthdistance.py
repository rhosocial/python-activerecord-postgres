# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/earthdistance.py
"""PostgreSQL earthdistance extension protocol.

This module defines the protocol for earthdistance extension support,
which provides earth distance calculations for geographic points.
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

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'earthdistance';
    - Programmatic detection: dialect.is_extension_installed('earthdistance')
    """

    def supports_earthdistance_type(self) -> bool:
        """Whether earth data type is supported.

        Requires earthdistance extension.
        Represents points on Earth's surface.
        """
        ...

    def supports_earthdistance_operators(self) -> bool:
        """Whether earthdistance operators are supported.

        Requires earthdistance extension.
        Supports <@> operator for distance in miles.
        """
        ...
