# src/rhosocial/activerecord/backend/impl/postgres/protocols/view.py
"""PostgreSQL view feature support protocol."""

from typing import Any, Protocol, runtime_checkable, Tuple


@runtime_checkable
class PostgresViewSupport(Protocol):
    """PostgreSQL view feature support protocol."""

    def supports_create_view_bind_parameters(self) -> bool:
        """Whether this statement's body may carry bind parameters.

        Defaults to ``False``: a DDL statement carries no bind parameters
        unless the dialect says it can, so a dialect that does not answer
        refuses rather than emitting a statement the engine may reject. A
        dialect whose engine accepts them returns ``True``.
        """
        ...  # pragma: no cover

    def supports_or_replace_view(self) -> bool: ...
    def supports_create_or_replace_view(self) -> bool: ...
    def supports_if_not_exists_view(self) -> bool: ...
    def supports_temporary_view(self) -> bool: ...
    def supports_if_exists_view(self) -> bool: ...
    def supports_view_check_option(self) -> bool: ...
    def supports_cascade_view(self) -> bool: ...
    def supports_restrict_view(self) -> bool: ...

    def format_create_view_statement(self, expr: Any) -> Tuple[str, tuple]:
        """Format CREATE VIEW statement for PostgreSQL.

        Supports TEMPORARY, OR REPLACE, column aliases, and
        WITH {LOCAL|CASCADED} CHECK OPTION.

        Args:
            expr: CreateViewStatement instance

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        ...
