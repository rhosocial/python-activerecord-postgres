# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/tablefunc.py
"""tablefunc extension protocol definition.

This module defines the protocol for tablefunc table functions
functionality in PostgreSQL.
"""
from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresTablefuncSupport(Protocol):
    """tablefunc table functions protocol.

    Feature Source: Extension support (requires tablefunc extension)

    tablefunc provides table functions:
    - crosstab() for pivot tables
    - crosstabN() for predefined column crosstabs
    - connectby() for hierarchical tree queries
    - normal_rand() for normal distribution random numbers

    Extension Information:
    - Extension name: tablefunc
    - Install command: CREATE EXTENSION tablefunc;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/tablefunc.html

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'tablefunc';
    - Programmatic detection: dialect.is_extension_installed('tablefunc')
    """

    def supports_tablefunc_crosstab(self) -> bool:
        """Whether crosstab functions are supported.

        Requires tablefunc extension.
        Enables pivot table operations.
        """
        ...

    def supports_tablefunc_connectby(self) -> bool:
        """Whether connectby function is supported.

        Requires tablefunc extension.
        Enables hierarchical tree queries.
        """
        ...

    def supports_tablefunc_normal_rand(self) -> bool:
        """Whether normal_rand function is supported.

        Requires tablefunc extension.
        Generates normal distribution random numbers.
        """
        ...
