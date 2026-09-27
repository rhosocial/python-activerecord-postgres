# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/type.py
"""PostgreSQL user-defined TYPE DDL protocol."""

from typing import List, Optional, Protocol, Tuple, Union, runtime_checkable

from rhosocial.activerecord.backend.dialect.protocols import UserDefinedTypeSupport
from rhosocial.activerecord.backend.expression.statements.ddl_type import DropTypeExpression


__all__ = ["PostgresTypeSupport"]


@runtime_checkable
class PostgresTypeSupport(UserDefinedTypeSupport, Protocol):
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

    def format_create_type_enum_statement(
        self,
        name: str,
        values: List[str],
        schema: Optional[str] = None,
    ) -> Tuple[str, tuple]:
        ...

    def format_drop_type_statement(
        self,
        expr: Optional[Union[DropTypeExpression, str]] = None,
        schema_name: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False,
        restrict: bool = False,
        *,
        schema: Optional[str] = None,
        name: Optional[str] = None,
    ) -> Tuple[str, tuple]:
        ...
