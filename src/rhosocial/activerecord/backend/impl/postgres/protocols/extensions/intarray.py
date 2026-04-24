# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/intarray.py
"""intarray extension protocol definition.

This module defines the protocol for intarray integer array operations
functionality in PostgreSQL.
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

    def format_intarray_function(self, function_name: str, *args) -> str:
        """Format an intarray function call."""
        ...

    def format_intarray_operator(self, column: str, operator: str, value: str) -> str:
        """Format an intarray operator expression."""
        ...

    def format_intarray_contains(self, column: str, values: list) -> str:
        """Format intarray contains check (column contains all values)."""
        ...

    def format_intarray_overlaps(self, column: str, values: list) -> str:
        """Format intarray overlaps check (any common elements)."""
        ...

    def format_intarray_idx(self, column: str, value: int) -> str:
        """Format idx function (find index of value in array)."""
        ...

    def format_intarray_subarray(self, column: str, start: int, length: int = None) -> str:
        """Format subarray function."""
        ...

    def format_intarray_uniq(self, column: str) -> str:
        """Format uniq function (remove duplicates)."""
        ...

    def format_intarray_sort(self, column: str, ascending: bool = True) -> str:
        """Format sort function."""
        ...

    def format_intarray_index_statement(self, table_name: str, column_name: str, index_name: Optional[str] = None) -> str:
        """Format CREATE INDEX statement with GIN intarray ops."""
        ...
