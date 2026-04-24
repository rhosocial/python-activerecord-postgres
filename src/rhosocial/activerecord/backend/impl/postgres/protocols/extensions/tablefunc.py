# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/tablefunc.py
"""tablefunc extension protocol definition.

This module defines the protocol for tablefunc table functions
functionality in PostgreSQL.
"""

from typing import Optional, Protocol, runtime_checkable


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

    def format_crosstab_function(self, sql: str, categories: Optional[str] = None) -> str:
        """Format crosstab function for pivot queries."""
        ...

    def format_connectby_function(
        self,
        table_name: str,
        key_column: str,
        parent_column: str,
        start_value: str,
        max_depth: Optional[int] = None,
        branch_delim: str = "~",
        order_by_column: Optional[str] = None,
    ) -> str:
        """Format connectby function for hierarchical queries."""
        ...

    def format_normal_rand_function(
        self, num_values: int, mean: float, stddev: float, seed: Optional[int] = None
    ) -> str:
        """Format normal_rand function for random values."""
        ...

    def format_crosstab_with_definition(
        self, source_sql: str, output_columns: str, categories_sql: Optional[str] = None
    ) -> str:
        """Format crosstab function with explicit column definition."""
        ...

    def format_connectby_full(
        self,
        table_name: str,
        key_column: str,
        parent_column: str,
        start_value: str,
        max_depth: int,
        branch_delim: str = "~",
    ) -> str:
        """Format connectby function with full options."""
        ...
