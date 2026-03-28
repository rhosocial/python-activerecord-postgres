# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/comment.py
"""PostgreSQL COMMENT ON protocol definition.

This module contains the PostgresCommentSupport protocol which defines
the interface for PostgreSQL's native COMMENT ON feature.
"""

from typing import Protocol, runtime_checkable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expressions import CommentExpression


@runtime_checkable
class PostgresCommentSupport(Protocol):
    """PostgreSQL COMMENT ON protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL supports commenting on database objects:
    - COMMENT ON TABLE
    - COMMENT ON COLUMN
    - COMMENT ON VIEW
    - COMMENT ON INDEX
    - COMMENT ON FUNCTION
    - COMMENT ON SCHEMA
    - etc.

    Official Documentation:
    - COMMENT: https://www.postgresql.org/docs/current/sql-comment.html

    Version Requirements:
    - All versions
    """

    def format_comment_statement(self, expr: "CommentExpression") -> Tuple[str, tuple]:
        """Format COMMENT ON statement.

        Args:
            expr: CommentExpression containing all options

        Returns:
            Tuple of (SQL string, parameters)
        """
        ...
