# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/hstore.py
"""hstore extension protocol definition.

hstore is a PostgreSQL extension that implements the hstore data type
for storing key-value pairs within a single PostgreSQL value.

See: https://www.postgresql.org/docs/current/hstore.html
"""

from typing import Dict, Optional, Protocol, Tuple, runtime_checkable


@runtime_checkable
class PostgresHstoreSupport(Protocol):
    """hstore key-value storage functionality protocol.

    hstore provides key-value pair data type:
    - hstore data type
    - Key-value operators
    - Index support

    Dependency requirements:
    - Extension name: hstore
    - Install command: CREATE EXTENSION hstore;
    - Minimum version: 1.0

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'hstore';
    - Programmatic detection: dialect.is_extension_installed('hstore')

    Documentation: https://www.postgresql.org/docs/current/hstore.html
    """

    def supports_hstore_type(self) -> bool:
        """Whether hstore data type is supported.

        Requires hstore extension.
        hstore is used to store key-value pair collections.
        """
        ...

    def supports_hstore_operators(self) -> bool:
        """Whether hstore operators are supported.

        Requires hstore extension.
        Supports operators like ->, ->>, @>, ?, etc.
        """
        ...

    def format_hstore_literal(self, data: Dict[str, str]) -> str:
        """Format an hstore literal value."""
        ...

    def format_hstore_constructor(self, pairs: list) -> str:
        """Format hstore constructor from array of key-value pairs."""
        ...

    def format_hstore_operator(
        self, column: str, operator: str, value: str, right_operand: Optional[str] = None
    ) -> str:
        """Format an hstore operator expression."""
        ...

    def format_hstore_get_value(self, column: str, key: str, as_text: bool = False) -> str:
        """Format hstore key value retrieval."""
        ...

    def format_hstore_contains_key(self, column: str, key: str) -> str:
        """Format hstore key existence check."""
        ...

    def format_hstore_contains_all_keys(self, column: str, keys: list) -> str:
        """Format hstore check for all keys existing."""
        ...

    def format_hstore_contains_any_keys(self, column: str, keys: list) -> str:
        """Format hstore check for any key existing."""
        ...

    def format_hstore_delete_key(self, hstore_expr: str, key: str) -> Tuple[str, tuple]:
        """Format delete(hstore, key) operation."""
        ...

    def format_hstore_each(self, hstore_expr: str) -> Tuple[str, tuple]:
        """Format each(hstore) expansion."""
        ...

    def format_hstore_keys(self, hstore_expr: str) -> Tuple[str, tuple]:
        """Format akeys(hstore) - get all keys as array."""
        ...

    def format_hstore_values(self, hstore_expr: str) -> Tuple[str, tuple]:
        """Format avals(hstore) - get all values as array."""
        ...
