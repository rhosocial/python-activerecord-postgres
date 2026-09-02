# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/rls_config.py
"""
PostgreSQL DDL expressions: Row-Level Security configuration.

PostgreSQL Documentation:
- ALTER TABLE: https://www.postgresql.org/docs/current/sql-altertable.html

Version Requirements:
- ENABLE / DISABLE ROW LEVEL SECURITY: PostgreSQL 9.5+
- ENABLE ALWAYS / FORCE / NO FORCE ROW LEVEL SECURITY: PostgreSQL 9.5+

Note: These statements are PostgreSQL extensions (non-SQL-standard). They
live in this backend package, together with the POLICY statements that build
the complete Row-Level Security ecosystem.
"""

from enum import Enum
from typing import Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "RlsConfigurationMode",
    "PostgresAlterTableRlsExpression",
    "PostgresForceRlsExpression",
]


class RlsConfigurationMode(Enum):
    """The ``ENABLE``/``DISABLE`` forms of row-level security on a table.

    PostgreSQL maps these onto ``ALTER TABLE ... ENABLE ROW LEVEL SECURITY``
    / ``ALTER TABLE ... DISABLE ROW LEVEL SECURITY``.
    """

    ENABLE = "ENABLE"
    DISABLE = "DISABLE"


class PostgresAlterTableRlsExpression(BaseExpression):
    """PostgreSQL ``ALTER TABLE ... {ENABLE|DISABLE} ROW LEVEL SECURITY``.

    A single expression drives both states through the ``mode`` field. The
    ``ENABLE ALWAYS`` variant is a separate keyword on the same statement and
    is produced by :attr:`always` (see class :class:`PostgresForceRlsExpression`
    for the related ``FORCE`` forms).

    Attributes:
        table: Name of the target table.
        schema: Optional schema for the table.
        mode: ``ENABLE`` or ``DISABLE``.
        always: If True (and ``mode`` is ENABLE), emit ``ENABLE ALWAYS``.
            ``ALWAYS`` has no meaning with ``DISABLE`` and raises an error.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect((14, 0, 0))
        >>> expr = PostgresAlterTableRlsExpression(
        ...     dialect, table="orders", mode=RlsConfigurationMode.ENABLE
        ... )
        >>> sql, params = expr.to_sql()  # doctest: +SKIP

    Raises:
        ValueError: if ``always`` is True with ``mode == DISABLE``.

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        table: str,
        mode: RlsConfigurationMode,
        schema: Optional[str] = None,
        always: bool = False,
    ):
        super().__init__(dialect)
        self.table = table
        self.schema = schema
        self.mode = mode
        self.always = always

    def to_sql(self) -> "Tuple[str, tuple]":
        """Generate the ALTER TABLE ... ROW LEVEL SECURITY statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_alter_table_rls_statement(self)


class PostgresForceRlsExpression(BaseExpression):
    """PostgreSQL ``ALTER TABLE ... {FORCE|NO FORCE} ROW LEVEL SECURITY``.

    ``FORCE ROW LEVEL SECURITY`` causes the table owner to also be subject to
    row-level security policies (the default exempts the table owner unless
    ``ENABLE ALWAYS``). ``NO FORCE`` reverts to the default behaviour. Both are
    PostgreSQL 9.5+.

    Attributes:
        table: Name of the target table.
        schema: Optional schema for the table.
        force: When True emit ``FORCE``; when False emit ``NO FORCE``.

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        table: str,
        force: bool = True,
        schema: Optional[str] = None,
    ):
        super().__init__(dialect)
        self.table = table
        self.schema = schema
        self.force = force

    def to_sql(self) -> "Tuple[str, tuple]":
        """Generate the FORCE/NO FORCE ROW LEVEL SECURITY statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_force_rls_statement(self)