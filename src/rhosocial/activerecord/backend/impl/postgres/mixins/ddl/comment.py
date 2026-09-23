# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/comment.py
"""PostgreSQL COMMENT ON DDL implementation."""

from typing import Tuple


class PostgresCommentMixin:
    """PostgreSQL COMMENT ON implementation."""

    def supports_comment_on(self) -> bool:
        """Whether standalone ``COMMENT ON`` statements are supported.

        PostgreSQL has no inline ``COMMENT`` syntax; comments are annotated
        through the standalone ``COMMENT ON`` statement (available since
        7.2). Always ``True``.
        """
        return True

    def format_comment_statement(self, expr) -> Tuple[str, tuple]:
        """Format COMMENT ON statement (PostgreSQL-specific).

        Accepts any comment expression carrying ``object_type``,
        ``object_name`` and ``comment``. The comment text is bound
        through a parameter placeholder (``IS ?`` / params); ``comment=None``
        renders ``IS NULL`` (clears the comment). Dotted names
        (``schema.table`` / ``table.column``) are quoted segment-by-segment
        so the rendered target stays a valid qualified reference.

        Args:
            expr: PostgresCommentExpression or CommentOnExpression carrying
                ``object_type``, ``object_name`` and ``comment``.

        Returns:
            Tuple of (SQL string, parameters)
        """
        if expr.comment is None:
            comment_value = "NULL"
        else:
            comment_value = self.get_parameter_placeholder()

        object_sql = ".".join(
            self.format_identifier(part) for part in str(expr.object_name).split(".")
        )
        parts = ["COMMENT ON", expr.object_type, object_sql]
        parts.append("IS")
        parts.append(comment_value)

        sql = " ".join(parts)

        if expr.comment is None:
            return sql, ()
        else:
            return sql, (expr.comment,)
