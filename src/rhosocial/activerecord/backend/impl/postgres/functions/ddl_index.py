# src/rhosocial/activerecord/backend/impl/postgres/functions/ddl_index.py
"""CREATE INDEX factory taking arbitrary arguments and returning a node.

The dialect's only index formatter is ``format_create_index_statement(self, expr)``.
Callers that used to pass scalar arguments to the dialect should build the node
here instead.
"""

from typing import Any, Dict, List, Optional, Sequence, Union

from rhosocial.activerecord.backend.expression.objects import Index, Table
from rhosocial.activerecord.backend.expression.operators import RawSQLPredicate

from ..expression.ddl import PostgresCreateIndexExpression, PostgresIndexType


def create_index_pg(
    dialect,
    index_name: str,
    table_name: str,
    columns: Sequence[Union[str, Any]],
    *,
    schema: Optional[str] = None,
    unique: bool = False,
    index_type: str = "btree",
    concurrently: bool = False,
    if_not_exists: bool = False,
    include_columns: Optional[List[str]] = None,
    with_options: Optional[Dict[str, Any]] = None,
    tablespace: Optional[str] = None,
    where_clause: Optional[Any] = None,
) -> PostgresCreateIndexExpression:
    """Build a ``CREATE INDEX`` expression from scalar arguments.

    This is a factory, not a formatter: any argument shape is fine here because
    the result is an expression node. Rendering stays in the one-to-one
    ``format_create_index_statement`` formatter.

    A string ``where_clause`` is wrapped in a raw predicate; pass a
    :class:`SQLPredicate` for a parameterized condition.
    """
    try:
        method = PostgresIndexType.normalize(index_type).value
    except ValueError as exc:
        raise ValueError(f"Invalid index_type: {index_type}. {exc}") from None

    where = None
    if where_clause is not None:
        where = (
            RawSQLPredicate(dialect, where_clause)
            if isinstance(where_clause, str)
            else where_clause
        )

    return PostgresCreateIndexExpression(
        dialect,
        index=Index(dialect, index_name, schema_name=schema),
        table=Table(dialect, table_name, schema_name=schema),
        columns=list(columns),
        unique=unique,
        if_not_exists=if_not_exists,
        index_type=method,
        where=where,
        include=list(include_columns) if include_columns else None,
        tablespace=tablespace,
        concurrent=concurrently,
        with_options=with_options,
    )


__all__ = ["create_index_pg"]
