# src/rhosocial/activerecord/backend/impl/postgres/functions/btree_indexes.py
"""Index factories for the btree_gin and btree_gist extensions."""

from rhosocial.activerecord.backend.expression.objects import Index, Table
from rhosocial.activerecord.backend.impl.postgres.expression import (
    PostgresCreateIndexExpression,
)


def btree_gin_index(dialect, table_name, columns, index_name, *, schema=None):
    """Build a GIN index expression over columns provided by btree_gin."""
    return PostgresCreateIndexExpression(
        dialect,
        index=Index(dialect, index_name, schema_name=schema),
        table=Table(dialect, table_name, schema_name=schema),
        columns=list(columns),
        index_type="gin",
    )


def btree_gist_index(
    dialect, table_name, columns, index_name, *, include=None, schema=None
):
    """Build a GiST index expression over columns provided by btree_gist."""
    return PostgresCreateIndexExpression(
        dialect,
        index=Index(dialect, index_name, schema_name=schema),
        table=Table(dialect, table_name, schema_name=schema),
        columns=list(columns),
        index_type="gist",
        include=list(include) if include else None,
    )


__all__ = ["btree_gin_index", "btree_gist_index"]
