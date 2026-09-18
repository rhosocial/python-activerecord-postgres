# src/rhosocial/activerecord/backend/impl/postgres/mixins/expression.py
"""PostgreSQL expression formatting mixin."""

from typing import Tuple


class PostgresExpressionMixin:
    """PostgreSQL-specific expression formatting."""

    def format_cast_expression(self, expr) -> Tuple[str, tuple]:
        """Format type cast expression using PostgreSQL :: syntax.

        PostgreSQL supports both standard CAST(expr AS type) syntax and the
        PostgreSQL-specific expr::type syntax. This method uses the more
        concise :: syntax which is idiomatic in PostgreSQL.

        ``expr.expression`` renders through its own ``to_sql()``; the cast
        target is the node's ``target_type`` construction parameter.

        Args:
            expr: :class:`~...expression.core.CastExpression` instance.

        Returns:
            Tuple of (SQL string, parameters)

        Example:
            >>> Column(dialect, 'price').cast('numeric').to_sql()
            # Returns: ('"price"::numeric', ())

        Note:
            For chained type conversions, each ::type is appended:
            >>> col.cast('money').cast('numeric').cast('float8')
            # Generates: "col"::money::numeric::float8

        """
        expr_sql, params = expr.expression.to_sql()
        sql = f"{expr_sql}::{expr.target_type}"
        if expr.alias:
            sql = f"{sql} AS {self.format_identifier(expr.alias)}"
        return sql, params

    def format_binary_operator(self, expr) -> Tuple[str, tuple]:
        """Format binary operator with psycopg placeholder escaping.

        psycopg uses %s as parameter placeholder. When the SQL operator itself
        contains % (e.g., pg_trgm similarity operator), it must be escaped as %%
        to prevent psycopg from interpreting it as a placeholder prefix.
        """
        left_sql, left_params = expr.left.to_sql()
        right_sql, right_params = expr.right.to_sql()
        op = expr.op
        # Escape % in operators for psycopg compatibility
        escaped_op = op.replace('%', '%%') if '%' in op else op
        sql = f"{left_sql} {escaped_op} {right_sql}"
        return sql, left_params + right_params
