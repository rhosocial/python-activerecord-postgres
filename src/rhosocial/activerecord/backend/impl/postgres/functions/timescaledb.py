# src/rhosocial/activerecord/backend/impl/postgres/functions/timescaledb.py
"""TimescaleDB function factories.

Only hypertable creation is wrapped in the first iteration; compression and
retention policies are separate concerns. Relation names are cast to REGCLASS
and column names are inline SQL literals so PostgreSQL resolves TimescaleDB's
``regclass`` / ``name`` signature without ambiguity.
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core
from rhosocial.activerecord.backend.impl.postgres.expression.types import PostgresRegClassType

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _inline(dialect: "SQLDialectBase", value) -> core.Parameter:
    """An inline literal, used for DDL positions that accept no bind parameters."""
    return value if isinstance(value, core.Parameter) else core.Parameter(dialect, value, inline=True)


def create_hypertable(
    dialect: "SQLDialectBase",
    relation,
    time_column,
    *,
    partitioning_column=None,
    number_partitions=None,
) -> core.FunctionCall:
    """Build ``create_hypertable(relation, time_column[, partition column, n])``.

    Args:
        dialect: The SQL dialect instance.
        relation: Relation name or expression, cast to REGCLASS.
        time_column: Name of the time column.
        partitioning_column: Optional space-partition column name.
        number_partitions: Number of space partitions; requires
            ``partitioning_column``.
    """
    relation_expr = relation if isinstance(relation, bases.BaseExpression) else core.Parameter(
        dialect, relation
    )
    arguments = [
        core.CastExpression(dialect, relation_expr, PostgresRegClassType(dialect)),
        _inline(dialect, time_column),
    ]
    if partitioning_column is not None:
        arguments.append(_inline(dialect, partitioning_column))
        if number_partitions is not None:
            if (
                not isinstance(number_partitions, int)
                or isinstance(number_partitions, bool)
                or number_partitions < 0
            ):
                raise ValueError("number_partitions must be a non-negative integer")
            arguments.append(core.Parameter(dialect, number_partitions))
    elif number_partitions is not None:
        raise ValueError("number_partitions requires partitioning_column")
    return core.FunctionCall(dialect, "create_hypertable", *arguments)


__all__ = ["create_hypertable"]
