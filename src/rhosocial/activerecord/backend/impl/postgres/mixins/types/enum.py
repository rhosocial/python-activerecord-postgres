# src/rhosocial/activerecord/backend/impl/postgres/mixins/types/enum.py
"""PostgreSQL ENUM compatibility formatting."""

from __future__ import annotations

from typing import Any, List, Optional, Sequence, Tuple, cast, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.statements.ddl_type import (
    AlterTypeExpression,
    CreateTypeExpression,
    DropTypeExpression,
)
from ...expression.ddl.type import (
    PostgresAddEnumValueAction,
    PostgresDropEnumTypeExpression,
    PostgresEnumTypeDefinition,
    PostgresRenameEnumValueAction,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase

__all__ = ["EnumTypeMixin"]


class EnumTypeMixin:
    if TYPE_CHECKING:
        name: str
        version: Tuple[int, int, int]

        def format_identifier(self, identifier: str, need_quote: bool = True) -> str: ...

        def inline_sql_literal(self, value: object) -> str: ...

        def format_create_type_statement(self, expr: CreateTypeExpression) -> Tuple[str, tuple]: ...

        def format_drop_type_statement(self, expr: DropTypeExpression) -> Tuple[str, tuple]: ...

        def format_alter_type_statement(self, expr: AlterTypeExpression) -> Tuple[str, tuple]: ...

    def _as_dialect(self) -> "SQLDialectBase":
        return cast("SQLDialectBase", self)

    def format_enum_type_name(
        self,
        name: str,
        schema: Optional[str] = None,
    ) -> Tuple[str, tuple]:
        value = f"{schema}.{name}" if schema is not None else name
        parts = value.split(".")
        if not parts or any(not part.strip() for part in parts):
            raise ValueError("ENUM type identifiers must contain non-empty segments")
        return ".".join(self.format_identifier(part) for part in parts), ()

    def format_enum_type_name_expression(self, expr: Any) -> Tuple[str, tuple]:
        return self.format_enum_type_name(expr.name, expr.schema)

    def format_enum_values(self, values: Sequence[str]) -> Tuple[str, tuple]:
        labels = list(values or [])
        if any(not isinstance(label, str) for label in labels):
            raise TypeError("ENUM values must be strings")
        return ", ".join(self.inline_sql_literal(label) for label in labels), ()

    def format_enum_values_expression(self, expr: Any) -> Tuple[str, tuple]:
        return self.format_enum_values(expr.values)

    def format_create_enum_type_raw(
        self,
        name: str,
        values: List[str],
        schema: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> Tuple[str, tuple]:
        dialect = self._as_dialect()
        expr = CreateTypeExpression(
            dialect,
            name,
            PostgresEnumTypeDefinition(dialect, values),
            schema_name=schema,
            if_not_exists=if_not_exists,
        )
        return self.format_create_type_statement(expr)

    def format_create_enum_type_raw_expression(self, expr: Any) -> Tuple[str, tuple]:
        return self.format_create_enum_type_raw(
            expr.name,
            expr.values,
            expr.schema,
            expr.if_not_exists,
        )

    def format_drop_enum_type_raw(
        self,
        name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False,
    ) -> Tuple[str, tuple]:
        expr = PostgresDropEnumTypeExpression(
            self._as_dialect(),
            name,
            schema=schema,
            if_exists=if_exists,
            cascade=cascade,
        )
        return self.format_drop_type_statement(expr)

    def format_drop_enum_type_raw_expression(self, expr: Any) -> Tuple[str, tuple]:
        return self.format_drop_enum_type_raw(
            expr.name,
            expr.schema,
            expr.if_exists,
            expr.cascade,
        )

    def format_alter_enum_add_value_raw(
        self,
        type_name: str,
        new_value: str,
        schema: Optional[str] = None,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ) -> Tuple[str, tuple]:
        dialect = self._as_dialect()
        action = PostgresAddEnumValueAction(
            dialect,
            new_value,
            before=before,
            after=after,
        )
        expr = AlterTypeExpression(dialect, type_name, [action], schema_name=schema)
        return self.format_alter_type_statement(expr)

    def format_alter_enum_add_value_raw_expression(self, expr: Any) -> Tuple[str, tuple]:
        return self.format_alter_enum_add_value_raw(
            expr.type_name,
            expr.new_value,
            expr.schema,
            expr.before,
            expr.after,
        )

    def create_enum_type(
        self,
        name: str,
        values: List[str],
        schema: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> str:
        sql, _ = self.format_create_enum_type_raw(
            name,
            values,
            schema,
            if_not_exists,
        )
        return sql

    def drop_enum_type(
        self,
        name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False,
    ) -> str:
        sql, _ = self.format_drop_enum_type_raw(name, schema, if_exists, cascade)
        return sql

    def alter_enum_add_value(
        self,
        type_name: str,
        new_value: str,
        schema: Optional[str] = None,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ) -> str:
        sql, _ = self.format_alter_enum_add_value_raw(
            type_name,
            new_value,
            schema,
            before,
            after,
        )
        return sql

    def format_create_enum_type(self, expr: Any) -> Tuple[str, tuple]:
        return self.format_create_enum_type_raw(
            expr.name,
            expr.values,
            expr.schema,
            expr.if_not_exists,
        )

    def format_drop_enum_type(self, expr: Any) -> Tuple[str, tuple]:
        return self.format_drop_enum_type_raw(
            expr.name,
            expr.schema,
            expr.if_exists,
            expr.cascade,
        )

    def format_alter_enum_add_value(self, expr: Any) -> Tuple[str, tuple]:
        return self.format_alter_enum_add_value_raw(
            expr.type_name,
            expr.new_value,
            expr.schema,
            expr.before,
            expr.after,
        )

    def format_enum_type_expression(self, expr: Any) -> Tuple[str, tuple]:
        return self.format_enum_type_name(expr.name, expr.schema)

    def format_alter_enum_type_add_value(self, expr: Any) -> Tuple[str, tuple]:
        return self.format_alter_enum_add_value_raw(
            expr.type_name,
            expr.new_value,
            expr.schema,
            expr.before,
            expr.after,
        )

    def format_alter_enum_type_rename_value(self, expr: Any) -> Tuple[str, tuple]:
        dialect = self._as_dialect()
        action = PostgresRenameEnumValueAction(
            dialect,
            expr.old_value,
            expr.new_value,
        )
        statement = AlterTypeExpression(
            dialect,
            expr.type_name,
            [action],
            schema_name=expr.schema,
        )
        return self.format_alter_type_statement(statement)
