"""intarray extension protocol definition.

This module defines the protocol for intarray integer array operations
functionality in PostgreSQL.

For SQL expression generation, use the function factories in
``functions/intarray.py`` instead of the removed format_* methods.
"""

from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class PostgresIntarraySupport(Protocol):
    """intarray integer array protocol.

    Feature Source: Extension support (requires intarray extension)

    intarray provides integer array operations:
    - Integer array operators (&&, @>, <@, @@, ~~)
    - Integer array functions (uniq, sort, idx, subarray)
    - GiST index support

    Extension Information:
    - Extension name: intarray
    - Install command: CREATE EXTENSION intarray;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/intarray.html

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'intarray';
    - Programmatic detection: dialect.is_extension_installed('intarray')
    """

    def supports_intarray_operators(self) -> bool:
        """Whether intarray operators are supported.

        Requires intarray extension.
        Supports operators: &&, @>, <@, @@, ~~.
        """
        ...

    def supports_intarray_functions(self) -> bool:
        """Whether intarray functions are supported.

        Requires intarray extension.
        Supports functions: uniq(), sort(), idx(), subarray().
        """
        ...

    def supports_intarray_index(self) -> bool:
        """Whether intarray indexes are supported.

        Requires intarray extension.
        Supports GiST indexes for integer arrays.
        """
        ...

    def format_intarray_index_statement(
        self, table_name: str, column_name: str, index_name: Optional[str] = None
    ) -> str:
        """Format CREATE INDEX statement with GIN intarray ops."""
        ...
