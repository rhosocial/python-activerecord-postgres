# src/rhosocial/activerecord/backend/impl/postgres/mixins/types/enum.py
"""PostgreSQL ENUM compatibility rendering.

Scalar argument handling lives in ``functions.enum`` factories, which return
expression nodes. This mixin only holds capability probes and the ``(self, expr)``
formatters that render those nodes.
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence, Tuple, cast, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.objects import Type
from rhosocial.activerecord.backend.expression.statements.ddl_type import (
    AlterTypeStatement,
    CreateTypeStatement,
    DropTypeStatement,
)
from ...expression.ddl.type import PostgresRenameEnumValueAction

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase

__all__ = ["EnumTypeMixin"]


class EnumTypeMixin:
    if TYPE_CHECKING:
        name: str
        version: Tuple[int, int, int]

        def format_identifier(self, identifier: str, need_quote: bool = True) -> str: ...

        def inline_sql_literal(self, value: object) -> str: ...

        def format_type_object(self, expr: Type) -> Tuple[str, tuple]: ...

        def format_create_type_statement(self, expr: CreateTypeStatement) -> Tuple[str, tuple]: ...

        def format_drop_type_statement(self, expr: DropTypeStatement) -> Tuple[str, tuple]: ...

        def format_alter_type_statement(self, expr: AlterTypeStatement) -> Tuple[str, tuple]: ...

    def _as_dialect(self) -> "SQLDialectBase":
        return cast("SQLDialectBase", self)

    def format_enum_values(self, expr: Sequence[str]) -> Tuple[str, tuple]:
        labels = list(expr or [])
        if any(not isinstance(label, str) for label in labels):
            raise TypeError("ENUM expr must be strings")
        return ", ".join(self.inline_sql_literal(label) for label in labels), ()

    def format_postgres_enum_values_expression(self, expr: Any) -> Tuple[str, tuple]:
        return self.format_enum_values(expr.values)

    def format_postgres_enum_type_name_expression(self, expr: Any) -> Tuple[str, tuple]:
        return self.format_type_object(Type(self, expr.name, schema_name=expr.schema))

    def format_enum_type_expression(self, expr: Any) -> Tuple[str, tuple]:
        return self.format_type_object(Type(self, expr.name, schema_name=expr.schema))

    def create_enum_type(
        self,
        name: str,
        values: List[str],
        schema: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> str:
        from ...functions.enum import create_enum_type as factory

        sql, _ = factory(
            self._as_dialect(), name, values, schema, if_not_exists=if_not_exists
        ).to_sql()
        return sql

    def drop_enum_type(
        self,
        name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False,
    ) -> str:
        from ...functions.enum import drop_enum_type as factory

        sql, _ = factory(
            self._as_dialect(), name, schema, if_exists=if_exists, cascade=cascade
        ).to_sql()
        return sql

    def alter_enum_add_value(
        self,
        type_name: str,
        new_value: str,
        schema: Optional[str] = None,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ) -> str:
        from ...functions.enum import add_enum_value as factory

        sql, _ = factory(
            self._as_dialect(), type_name, new_value, schema,
            before=before, after=after,
        ).to_sql()
        return sql

    def format_create_enum_type(self, expr: Any) -> Tuple[str, tuple]:
        from ...functions.enum import create_enum_type as factory

        return factory(
            self._as_dialect(), expr.name, expr.values, expr.schema,
            if_not_exists=expr.if_not_exists,
        ).to_sql()

    def format_drop_enum_type(self, expr: Any) -> Tuple[str, tuple]:
        from ...functions.enum import drop_enum_type as factory

        return factory(
            self._as_dialect(), expr.name, expr.schema,
            if_exists=expr.if_exists, cascade=expr.cascade,
        ).to_sql()

    def format_alter_enum_add_value(self, expr: Any) -> Tuple[str, tuple]:
        from ...functions.enum import add_enum_value as factory

        return factory(
            self._as_dialect(), expr.type_name, expr.new_value, expr.schema,
            before=expr.before, after=expr.after,
        ).to_sql()

    def format_alter_enum_type_add_value(self, expr: Any) -> Tuple[str, tuple]:
        return self.format_alter_enum_add_value(expr)

    def format_alter_enum_type_rename_value(self, expr: Any) -> Tuple[str, tuple]:
        dialect = self._as_dialect()
        action = PostgresRenameEnumValueAction(
            dialect,
            expr.old_value,
            expr.new_value,
        )
        statement = AlterTypeStatement(
            dialect,
            Type(dialect, expr.type_name, schema_name=expr.schema),
            [action],
        )
        return self.format_alter_type_statement(statement)
