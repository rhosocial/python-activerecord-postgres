# src/rhosocial/activerecord/backend/impl/postgres/protocols/dml/copy.py
"""Runtime protocol for PostgreSQL COPY expression support."""

from typing import Any, Protocol, runtime_checkable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.copy import PostgresCopyFromExpression, PostgresCopyToExpression


@runtime_checkable
class PostgresCopySupport(Protocol):
    """PostgreSQL client-stream COPY protocol."""

    def supports_copy_to(self) -> bool:
        """Whether COPY TO STDOUT is supported."""
        ...

    def supports_copy_from(self) -> bool:
        """Whether COPY FROM STDIN is supported."""
        ...

    def supports_copy_json(self) -> bool:
        """Whether COPY TO JSON is supported."""
        ...

    def supports_copy_from_json(self) -> bool:
        """Whether COPY FROM JSON is supported; PostgreSQL never supports it."""
        ...

    def supports_copy_force_array(self) -> bool:
        """Whether COPY TO FORCE_ARRAY is supported."""
        ...

    def supports_copy_force_all(self) -> bool:
        """Whether COPY FROM FORCE_NOT_NULL/FORCE_NULL accept '*'."""
        ...

    def supports_copy_partitioned_table_to(self) -> bool:
        """Whether direct COPY TO supports partitioned tables."""
        ...

    def supports_copy_text_header(self) -> bool:
        """Whether COPY text HEADER is supported."""
        ...

    def supports_copy_header_match(self) -> bool:
        """Whether COPY FROM HEADER MATCH is supported."""
        ...

    def supports_copy_header_line_count(self) -> bool:
        """Whether numeric COPY HEADER line counts are supported."""
        ...

    def supports_copy_on_error(self) -> bool:
        """Whether COPY FROM ON_ERROR is supported."""
        ...

    def supports_copy_reject_limit(self) -> bool:
        """Whether COPY FROM REJECT_LIMIT is supported."""
        ...

    def supports_copy_set_null(self) -> bool:
        """Whether COPY FROM ON_ERROR SET_NULL is supported."""
        ...

    def supports_copy_log_verbosity(self) -> bool:
        """Whether COPY FROM LOG_VERBOSITY is supported."""
        ...

    def supports_copy_silent_log(self) -> bool:
        """Whether COPY FROM LOG_VERBOSITY SILENT is supported."""
        ...

    def format_copy_to_statement(
        self,
        expr: "PostgresCopyToExpression",
    ) -> Tuple[str, Tuple[Any, ...]]:
        """Format COPY TO STDOUT."""
        ...

    def format_copy_from_statement(
        self,
        expr: "PostgresCopyFromExpression",
    ) -> Tuple[str, Tuple[Any, ...]]:
        """Format COPY FROM STDIN."""
        ...
