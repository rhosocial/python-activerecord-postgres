# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/alter_column.py
"""PostgreSQL-specific ALTER TABLE column actions.

PostgreSQL's ``ALTER COLUMN ... SET DATA TYPE ... USING <expression>`` accepts a
conversion expression that has no generic equivalent:

* ``USING (<expression>)`` — how to convert existing values to the new type.

It lives on ``PostgresAlterColumn`` (deriving the generic ``AlterColumn``) and
is rendered by the PostgreSQL ``format_alter_column_action`` override.
PostgreSQL does **not** share it with any other backend.
"""

from typing import Any, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.statements import (
    AlterColumn,
    ColumnAlterOperation,
)

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "PostgresAlterColumn",
]


class PostgresAlterColumn(AlterColumn):
    """A PostgreSQL ``ALTER COLUMN`` action extending the generic one.

    Adds the PostgreSQL-only ``using`` conversion expression for
    ``SET DATA TYPE``.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        column_name: str,
        operation: Union[ColumnAlterOperation, str],
        *,
        new_value: Any = None,
        cascade: bool = False,
        using: Any = None,
    ):
        super().__init__(
            dialect,
            column_name,
            operation,
            new_value=new_value,
            cascade=cascade,
        )
        self.using = using
