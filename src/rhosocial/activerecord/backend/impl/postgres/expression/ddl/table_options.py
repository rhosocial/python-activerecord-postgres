# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/table_options.py
"""PostgreSQL-specific CREATE TABLE options.

PostgreSQL adds the ``UNLOGGED`` header modifier to ``CREATE TABLE``, which
has no generic equivalent. It lives on ``PostgresCreateTableOptions`` (deriving
the generic ``CreateTableOptions``) and is rendered by the PostgreSQL
``format_create_table_options`` override.
"""

from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.statements import CreateTableOptions

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "PostgresCreateTableOptions",
]


class PostgresCreateTableOptions(CreateTableOptions):
    """A PostgreSQL CREATE TABLE options declaration extending the generic one.

    Adds the PostgreSQL-only ``unlogged`` header modifier.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        *,
        or_replace: bool = False,
        comment: Optional[str] = None,
        unlogged: bool = False,
    ):
        super().__init__(dialect, or_replace=or_replace, comment=comment)
        self.unlogged = unlogged
