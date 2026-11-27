# src/rhosocial/activerecord/backend/impl/postgres/mixins/expression.py
"""PostgreSQL expression formatting mixin."""

from typing import Tuple, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.core import FunctionCall, RowCountWildcard
    from ..expression.values import (
        PostgresHstoreValue,
        PostgresLtreeValue,
        PostgresNetworkValue,
        PostgresRangeValue,
    )


def _escape_psycopg_operator(operator: str) -> str:
    """Double a literal ``%`` in an operator symbol for psycopg.

    psycopg reads ``%`` as the start of a bind placeholder, so an operator that
    contains one (for example a pg_trgm symbol) must be doubled before it is
    concatenated. This escapes the declared operator atom, not assembled SQL.
    """
    return operator.replace("%", "%%") if "%" in operator else operator


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
            >>> Column(dialect, 'price').cast(DecimalType(dialect)).to_sql()
            # Returns: ('"price"::numeric', ())

        Note:
            For chained type conversions, each ::type is appended:
            >>> col.cast(PostgresMoneyType(self.dialect)).cast(DecimalType(self.dialect)).cast(FloatType(self.dialect))
            # Generates: "col"::money::numeric::float8

        """
        expr.validate_target_type()
        expr_sql, params = expr.expression.to_sql()
        # The target is an expression, so it renders itself and this
        # concatenates. Interpolating it would have produced
        # `col::IntegerType()` instead of `col::INTEGER`.
        type_sql, _ = expr.target_type.to_sql()
        sql = f"{expr_sql}::{type_sql}"
        if expr.alias:
            sql = f"{sql} AS {self.format_identifier(expr.alias)}"
        return sql, params

    def format_binary_expression(self, expr) -> Tuple[str, tuple]:
        """Format binary operator with psycopg placeholder escaping.

        psycopg uses %s as parameter placeholder. When the SQL operator itself
        contains % (e.g., pg_trgm similarity operator), it must be escaped as %%
        to prevent psycopg from interpreting it as a placeholder prefix.

        The operator is spelled through ``.symbol`` when the node carries the
        comparison vocabulary: §13.1 split the word from the character, and the
        word (``EQ``, ``NE``...) is what the artifact holds. A plain string
        operator -- the pg_trgm and range operators are their own symbols and
        have no word -- is used as it is.
        """
        from rhosocial.activerecord.backend.expression.predicates import (
            ComparisonOperator,
        )

        left_sql, left_params = expr.left.to_sql()
        right_sql, right_params = expr.right.to_sql()
        op = expr.op
        if isinstance(op, ComparisonOperator):
            op = op.symbol
        # Escape the declared operator atom, not the assembled SQL.
        escaped_op = _escape_psycopg_operator(op)
        sql = f"{left_sql} {escaped_op} {right_sql}"
        return sql, left_params + right_params

    def format_row_count_wildcard(self, expr: "RowCountWildcard") -> Tuple[str, tuple]:
        """Format ``PostgresRowCountWildcard``: ``*`` or ``t.*``.

        Core's bare ``*`` is right for ``COUNT(*)`` and is left alone; the override
        exists for the qualified form, which core's ``RowCountWildcard``
        deliberately does not carry because standard SQL has no use for it.

        The check is a class test, not a missing-attribute probe: core's node
        has no ``relation`` at all, and asking for one anyway is exactly the
        reflection rule 9.8 forbids. Anything that is not this backend's
        qualified form renders the bare asterisk.

        Args:
            expr: :class:`~...expression.wildcard.PostgresRowCountWildcard`.

        Returns:
            A ``(sql, params)`` tuple; ``params`` is empty.

        Raises:
            TypeError: ``relation`` is set but is not a ``NamedRelationRef``.
        """
        from rhosocial.activerecord.backend.impl.postgres.expression.wildcard import (
            PostgresRowCountWildcard,
        )

        if not isinstance(expr, PostgresRowCountWildcard):
            return "*", ()
        relation = expr.relation
        if relation is None:
            return "*", ()
        from rhosocial.activerecord.backend.expression.sources import NamedRelationRef

        if not isinstance(relation, NamedRelationRef):
            raise TypeError(
                f"PostgresRowCountWildcard.relation must be a NamedRelationRef, "
                f"got {type(relation).__name__}"
            )
        if relation.alias:
            qualifier = self.format_identifier(relation.alias, relation.alias_need_quote)
        else:
            qualifier, _ = relation.relation.to_sql()
        return f"{qualifier}.*", ()

    def format_truncate_function(self, expr) -> Tuple[str, tuple]:
        """Format ``TRUNC`` -- PostgreSQL's spelling of the core's TRUNCATE node.

        The core's :class:`~...expression.functions.math.TruncateFunction`
        writes the SQL-standard ``TRUNCATE``; in PostgreSQL that word is the
        DDL statement that empties a table, and the numeric function is
        spelled ``TRUNC``. Measured live 2026-10-18 against 17.11 (Debian
        17.11-1.pgdg13+2): ``SELECT TRUNC(3.716, 2)`` answers ``3.71`` while
        ``SELECT TRUNCATE(3.716)`` is refused with ``UndefinedFunction:
        function truncate(numeric) does not exist``. The node and its operand
        list are unchanged; only the spelled name differs, which is exactly
        the spelling override the one-to-one rule allows.

        Args:
            expr: :class:`~...expression.functions.math.TruncateFunction`.

        Returns:
            A ``(sql, params)`` tuple.
        """
        parts_sql, parts_params = [], []
        for operand in expr.arguments:
            operand_sql, operand_params = operand.to_sql()
            parts_sql.append(operand_sql)
            parts_params.extend(operand_params)
        joined_sql = ", ".join(parts_sql)
        sql = f"TRUNC({joined_sql})"
        if expr.alias:
            sql = f"{sql} AS {self.format_identifier(expr.alias)}"
        return sql, tuple(parts_params)

    def format_function_call(
        self,
        expr: Union[
            "FunctionCall",
            "PostgresNetworkValue",
            "PostgresRangeValue",
            "PostgresHstoreValue",
            "PostgresLtreeValue",
        ],
    ) -> Tuple[str, tuple]:
        """Render a call, including PostgreSQL's typed value wrappers.

        The four value nodes declare this same formatter but render the call
        they wrap through their own ``to_sql()``, so the annotation names both
        the call and the wrappers instead of a bare ``BaseExpression``. Every
        ordinary call still goes through core's implementation below it.
        """
        from ..expression.values import (
            PostgresHstoreValue,
            PostgresLtreeValue,
            PostgresNetworkValue,
            PostgresRangeValue,
        )

        if isinstance(
            expr,
            (
                PostgresNetworkValue,
                PostgresRangeValue,
                PostgresHstoreValue,
                PostgresLtreeValue,
            ),
        ):
            return expr.to_sql()
        return super().format_function_call(expr)
