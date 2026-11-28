# src/rhosocial/activerecord/backend/impl/postgres/functions/bloom.py
"""bloom index factory."""

from rhosocial.activerecord.backend.expression.objects import Index, Table

from ..expression.statements.ddl import PostgresCreateIndexExpression


def bloom_index(
    dialect,
    table_name: str,
    columns: list,
    index_name: str,
    *,
    fill_factor: int = None,
    schema: str = None,
) -> PostgresCreateIndexExpression:
    """Build a bloom index expression (extension access method)."""
    return PostgresCreateIndexExpression(
        dialect,
        index=Index(dialect, index_name, schema_name=schema),
        table=Table(dialect, table_name, schema_name=schema),
        columns=list(columns),
        index_type="BLOOM",
        with_options={"fillfactor": fill_factor} if fill_factor is not None else None,
    )


__all__ = ["bloom_index"]
