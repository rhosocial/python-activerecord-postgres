# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/comment.py
"""PostgreSQL COMMENT ON DDL implementation."""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expressions import CommentExpression


class PostgresCommentMixin:
    """PostgreSQL COMMENT ON implementation."""

    def format_comment_statement(self, expr: "CommentExpression") -> Tuple[str, tuple]:
        """Format COMMENT ON statement (PostgreSQL-specific).

        Args:
            expr: CommentExpression containing all options

        Returns:
            Tuple of (SQL string, parameters)
        """
        if expr.comment is None:
            comment_value = "NULL"
        else:
            comment_value = self.get_parameter_placeholder()

        parts = ["COMMENT ON", expr.object_type, self.format_identifier(expr.object_name)]
        parts.append("IS")
        parts.append(comment_value)

        sql = " ".join(parts)

        if expr.comment is None:
            return sql, ()
        else:
            return sql, (expr.comment,)
