# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/repack.py
"""Runtime protocol for native PostgreSQL REPACK support."""

from typing import Protocol, runtime_checkable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.ddl.repack import PostgresRepackExpression


@runtime_checkable
class PostgresRepackSupport(Protocol):
    """PostgreSQL 19 native REPACK protocol."""

    def supports_repack(self) -> bool:
        """Whether native REPACK is supported."""
        ...

    def supports_repack_all_tables(self) -> bool:
        """Whether whole-database REPACK is supported."""
        ...

    def supports_repack_using_index(self) -> bool:
        """Whether REPACK USING INDEX is supported."""
        ...

    def supports_repack_concurrently(self) -> bool:
        """Whether REPACK CONCURRENTLY is supported."""
        ...

    def supports_repack_analyze(self) -> bool:
        """Whether REPACK ANALYZE is supported."""
        ...

    def format_repack_statement(
        self,
        expr: "PostgresRepackExpression",
    ) -> Tuple[str, tuple]:
        """Format a native REPACK statement."""
        ...
