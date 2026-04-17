# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/comment.py
"""
PostgreSQL DDL expressions: COMMENT operations.

PostgreSQL Documentation:
- COMMENT: https://www.postgresql.org/docs/current/sql-comment.html

Version Requirements:
- COMMENT: PostgreSQL 7.2+
"""

from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = ["CommentExpression"]


class CommentExpression(BaseExpression):
    """PostgreSQL COMMENT ON statement expression.

    Stores a comment about a database object.
    Comments can be retrieved using pg_descr objects.

    Attributes:
        object_type: Object type: 'TABLE', 'COLUMN', 'INDEX', 'VIEW',
                    'SCHEMA', 'FUNCTION', 'TRIGGER', etc.
        object_name: Name of the object to comment on.
                   For COLUMN, format as 'table.column'.
        comment: Comment text (None to remove existing comment).
        schema: Schema name for the object.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> # Comment on a table
        >>> comment = CommentExpression(
        ...     dialect=dialect,
        ...     object_type="TABLE",
        ...     object_name="users",
        ...     comment="User accounts table",
        ... )
        >>> sql, params = comment.to_sql()
        >>> sql
        "COMMENT ON TABLE users IS 'User accounts table'"

        >>> # Comment on a column
        >>> comment = CommentExpression(
        ...     dialect=dialect,
        ...     object_type="COLUMN",
        ...     object_name="users.email",
        ...     comment="User email address",
        ... )

        >>> # Remove comment
        >>> comment = CommentExpression(
        ...     dialect=dialect,
        ...     object_type="INDEX",
        ...     object_name="users_email_idx",
        ...     comment=None,
        ... )
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        object_type: str,
        object_name: str,
        comment: Optional[str],
        schema: Optional[str] = None,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.object_type = object_type
        self.object_name = object_name
        self.comment = comment
        self.schema = schema
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate COMMENT ON SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        return self.dialect.format_comment_statement(self)