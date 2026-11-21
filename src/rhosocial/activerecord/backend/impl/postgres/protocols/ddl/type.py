# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/type.py
"""PostgreSQL user-defined TYPE DDL protocol."""

from typing import Protocol, Tuple, runtime_checkable

from rhosocial.activerecord.backend.dialect.protocols import (
    AlterTypeSupport,
    CreateTypeSupport,
    DropTypeSupport,
    TypeObjectSupport,
)
from rhosocial.activerecord.backend.expression.statements.ddl_type import DropTypeStatement


__all__ = ["PostgresTypeSupport"]


@runtime_checkable
class PostgresTypeSupport(
    CreateTypeSupport,
    AlterTypeSupport,
    DropTypeSupport,
    TypeObjectSupport,
    Protocol,
):
    def supports_drop_type_cascade(self) -> bool:
        ...

    def supports_drop_type_restrict(self) -> bool:
        ...

    def supports_type_if_not_exists(self) -> bool:
        ...

    def supports_type_if_exists(self) -> bool:
        ...

    def supports_type_cascade(self) -> bool:
        ...

    def format_postgres_drop_type_name_expression(self, expr) -> Tuple[str, tuple]: ...

    def format_drop_type_statement(self, expr: DropTypeStatement) -> Tuple[str, tuple]:
        ...
