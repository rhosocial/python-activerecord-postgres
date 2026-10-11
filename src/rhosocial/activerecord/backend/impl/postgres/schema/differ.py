# src/rhosocial/activerecord/backend/impl/postgres/schema/differ.py
"""PostgreSQL schema differ — column order has no semantic meaning."""

from rhosocial.activerecord.backend.schema.differ import SchemaDiffer


class PostgresSchemaDiffer(SchemaDiffer):
    """PostgreSQL schema differ.

    Column order has no semantic meaning in PostgreSQL, so the default
    ``_columns_equivalent`` — which compares parsed data types by
    identity — is sufficient and needs no override here.

    One consequence worth knowing: because PostgreSQL normalises every
    array declaration to a single dimension on storage, an introspected
    array column always reports ``dimensions=1``. A snapshot compared
    against one built with ``dimensions=2`` therefore reports a type
    difference that the database does not itself consider one.
    """
    pass
