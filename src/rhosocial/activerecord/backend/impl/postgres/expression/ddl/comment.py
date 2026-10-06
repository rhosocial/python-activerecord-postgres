# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/comment.py
"""
PostgreSQL DDL expressions: COMMENT operations.

PostgreSQL Documentation:
- COMMENT: https://www.postgresql.org/docs/current/sql-comment.html

Version Requirements:
- COMMENT: PostgreSQL 7.2+
"""

from typing import Optional, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.objects import SchemaObject
from rhosocial.activerecord.backend.expression.statements.ddl_comment import (
    CommentObjectType,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = ["PostgresCommentExpression"]


class PostgresCommentExpression(BaseExpression):
    """PostgreSQL COMMENT ON statement expression.

    Stores a comment about a database object.
    Comments can be retrieved using pg_descr objects.

    Attributes:
        object_type: Object type: 'TABLE', 'COLUMN', 'INDEX', 'VIEW',
                    'SCHEMA', 'FUNCTION', 'TRIGGER', etc.
        object: The schema object being commented on. Its own namespace is
                   rendered by the dialect, so a qualified name needs no
                   assembly here.
        comment: Comment text (None to remove existing comment).
        column: Name of the column, when the target is a column of ``object``.

    Example:
        >>> from rhosocial.activerecord.backend.expression.objects import Table
        >>> from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> # Comment on a table
        >>> comment = PostgresCommentExpression(
        ...     dialect=dialect,
        ...     object_type="TABLE",
        ...     object=Table(dialect, "users"),
        ...     comment="User accounts table",
        ... )
        >>> sql, params = comment.to_sql()
        >>> sql
        'COMMENT ON TABLE "users" IS ?'

        >>> # Comment on a column
        >>> comment = PostgresCommentExpression(
        ...     dialect=dialect,
        ...     object_type="COLUMN",
        ...     object=Table(dialect, "users"),
        ...     column="email",
        ...     comment="User email address",
        ... )

        >>> # Remove comment
        >>> comment = PostgresCommentExpression(
        ...     dialect=dialect,
        ...     object_type="INDEX",
        ...     object=Table(dialect, "users"),
        ...     comment=None,
        ... )

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        object_type: Union["CommentObjectType", str],
        object: "SchemaObject",
        comment: Optional[str] = None,
        column: Optional[str] = None,
    ):
        super().__init__(dialect)
        if not isinstance(object, SchemaObject):
            raise TypeError(
                f"object must be a SchemaObject, got {type(object).__name__}"
            )
        if column is not None and (not isinstance(column, str) or not column.strip()):
            raise ValueError("column must be a non-empty string or None")
        self.object_type = object_type
        self.object = object
        self.comment = comment
        self.column = column

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_comment_statement"