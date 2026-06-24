# src/rhosocial/activerecord/backend/impl/postgres/schema/differ.py
"""PostgreSQL schema differ — column order has no semantic meaning."""

from rhosocial.activerecord.backend.schema.differ import SchemaDiffer


class PostgresSchemaDiffer(SchemaDiffer):
    """PostgreSQL schema differ.

    Column order has no semantic meaning in PostgreSQL, and array
    dimension equivalence is already handled by
    ``PostgresArrayType.is_equivalent()``, so the default
    ``_columns_equivalent`` is sufficient.
    """
    pass
