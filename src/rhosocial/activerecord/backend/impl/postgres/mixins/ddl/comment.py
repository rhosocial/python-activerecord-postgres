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

        Accepts a :class:`~rhosocial.activerecord.backend.expression.objects.SchemaObject`
        holder carrying ``object_type``, the ``object`` being annotated,
        ``comment`` and an optional ``column``. The object renders itself, so a
        qualified name comes out of the namespace machinery rather than being
        assembled here; a ``column`` is appended after it when the target is a
        column of that object. The comment text is bound through a parameter
        placeholder (``IS ?`` / params); ``comment=None`` renders ``IS NULL``
        (clears the comment).

        Args:
            expr: ``CommentOnExpression`` carrying ``object_type``, ``object``,
                ``comment`` and an optional ``column``.

        Returns:
            Tuple of (SQL string, parameters)
        """
        if expr.comment is None:
            comment_value = "NULL"
        else:
            comment_value = self.get_parameter_placeholder()

        object_sql, object_params = expr.object.to_sql()
        if expr.column:
            object_sql = f"{object_sql}.{self.format_identifier(expr.column)}"

        object_type = getattr(expr.object_type, "value", expr.object_type)
        parts = ["COMMENT ON", object_type, object_sql]
        parts.append("IS")
        parts.append(comment_value)

        sql = " ".join(parts)

        if expr.comment is None:
            return sql, object_params
        return sql, tuple(object_params) + (expr.comment,)
