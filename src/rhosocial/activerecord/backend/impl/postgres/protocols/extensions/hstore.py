"""hstore extension protocol definition.

hstore is a PostgreSQL extension that implements the hstore data type
for storing key-value pairs within a single PostgreSQL value.

For SQL expression generation, use the function factories in
``functions/hstore.py`` instead of the removed format_* methods.

See: https://www.postgresql.org/docs/current/hstore.html
"""

from typing import Protocol, runtime_checkable


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
