# src/rhosocial/activerecord/backend/impl/postgres/mixins/types/enum.py
"""PostgreSQL ENUM type support mixin.

This module provides the EnumTypeMixin class for handling PostgreSQL
enumerated type operations.
"""

from typing import Optional, List, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.impl.postgres.expression.ddl.type import (
        PostgresCreateEnumTypeExpression,
        PostgresDropEnumTypeExpression,
        PostgresAlterEnumAddValueExpression,
        EnumTypeNameExpression,
        EnumValuesExpression,
        CreateEnumTypeExpression,
        DropEnumTypeExpression,
        AlterEnumAddValueExpression,
    )


class EnumTypeMixin:
    """Mixin providing PostgreSQL ENUM type formatting methods.

    This mixin implements the EnumTypeSupport protocol.
    """

    def format_enum_type_name(self, name: str, schema: Optional[str] = None) -> Tuple[str, tuple]:
        """Format enum type name with optional schema.

        Args:
            name: Type name
            schema: Optional schema name

        Returns:
            Tuple of (formatted type name, empty params tuple)

        """
        if schema:
            return (f"{schema}.{name}", ())
        return (name, ())

    def format_enum_type_name_expression(self, expr: "EnumTypeNameExpression") -> Tuple[str, tuple]:
        """Format enum type name from expression object.

        Args:
            expr: :class:`EnumTypeNameExpression` instance.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.format_enum_type_name(expr.name, expr.schema)

    def format_enum_values(self, values: List[str]) -> Tuple[str, tuple]:
        """Format enum values list for SQL.

        Args:
            values: List of enum values

        Returns:
            Tuple of (SQL-formatted values string, empty params tuple)

        """
        return (", ".join(f"'{v}'" for v in values), ())

    def format_enum_values_expression(self, expr: "EnumValuesExpression") -> Tuple[str, tuple]:
        """Format enum values list from expression object.

        Args:
            expr: :class:`EnumValuesExpression` instance.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.format_enum_values(expr.values)

    def format_create_enum_type_raw(
        self, name: str, values: List[str], schema: Optional[str] = None, if_not_exists: bool = False
    ) -> Tuple[str, tuple]:
        """Format CREATE TYPE statement for enum.

        Args:
            name: Type name
            values: Allowed values
            schema: Optional schema
            if_not_exists: Add IF NOT EXISTS

        Returns:
            Tuple of (SQL statement string, empty params tuple)

        """
        full_name, _ = self.format_enum_type_name(name, schema)
        values_str, _ = self.format_enum_values(values)
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        return (f"CREATE TYPE {exists_clause}{full_name} AS ENUM ({values_str})", ())

    def format_create_enum_type_raw_expression(self, expr: "CreateEnumTypeExpression") -> Tuple[str, tuple]:
        """Format CREATE TYPE ... AS ENUM from expression object.

        Args:
            expr: :class:`CreateEnumTypeExpression` instance.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.format_create_enum_type_raw(
            expr.name,
            expr.values,
            expr.schema,
            expr.if_not_exists,
        )

    def format_drop_enum_type_raw(
        self, name: str, schema: Optional[str] = None, if_exists: bool = False, cascade: bool = False
    ) -> Tuple[str, tuple]:
        """Format DROP TYPE statement.

        Args:
            name: Type name
            schema: Optional schema
            if_exists: Add IF EXISTS
            cascade: Add CASCADE

        Returns:
            Tuple of (SQL statement string, empty params tuple)

        """
        full_name, _ = self.format_enum_type_name(name, schema)
        exists_clause = "IF EXISTS " if if_exists else ""
        cascade_clause = " CASCADE" if cascade else ""
        return (f"DROP TYPE {exists_clause}{full_name}{cascade_clause}", ())

    def format_drop_enum_type_raw_expression(self, expr: "DropEnumTypeExpression") -> Tuple[str, tuple]:
        """Format DROP TYPE from expression object.

        Args:
            expr: :class:`DropEnumTypeExpression` instance.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.format_drop_enum_type_raw(
            expr.name,
            expr.schema,
            expr.if_exists,
            expr.cascade,
        )

    def format_alter_enum_add_value_raw(
            self, type_name: str, new_value: str, schema: Optional[str] = None,
            before: Optional[str] = None, after: Optional[str] = None
        ) -> Tuple[str, tuple]:
        """Format ALTER TYPE ADD VALUE statement.

        Args:
            type_name: Type name
            new_value: New value to add
            schema: Optional schema
            before: Add before this value
            after: Add after this value

        Returns:
            Tuple of (SQL statement string, empty params tuple)

        """
        full_name, _ = self.format_enum_type_name(type_name, schema)
        sql = f"ALTER TYPE {full_name} ADD VALUE '{new_value}'"
        if before:
            sql += f" BEFORE '{before}'"
        elif after:
            sql += f" AFTER '{after}'"
        return (sql, ())

    def format_alter_enum_add_value_raw_expression(self, expr: "AlterEnumAddValueExpression") -> Tuple[str, tuple]:
        """Format ALTER TYPE ADD VALUE from expression object.

        Args:
            expr: :class:`AlterEnumAddValueExpression` instance.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.format_alter_enum_add_value_raw(
            expr.type_name,
            expr.new_value,
            expr.schema,
            expr.before,
            expr.after,
        )

    # =========================================================================
    # Convenience methods for EnumTypeSupport protocol
    # =========================================================================

    def create_enum_type(
        self, name: str, values: List[str], schema: Optional[str] = None, if_not_exists: bool = False
    ) -> str:
        """Generate CREATE TYPE statement for enum.

        Implements EnumTypeSupport protocol.

        Args:
            name: Enum type name
            values: List of allowed values
            schema: Optional schema name
            if_not_exists: Add IF NOT EXISTS clause (PG 9.1+)

        Returns:
            SQL statement string

        """
        sql, _ = self.format_create_enum_type_raw(name, values, schema, if_not_exists)
        return sql

    def drop_enum_type(
        self, name: str, schema: Optional[str] = None, if_exists: bool = False, cascade: bool = False
    ) -> str:
        """Generate DROP TYPE statement for enum.

        Implements EnumTypeSupport protocol.

        Args:
            name: Enum type name
            schema: Optional schema name
            if_exists: Add IF EXISTS clause
            cascade: Add CASCADE clause

        Returns:
            SQL statement string

        """
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
        """Generate ALTER TYPE ADD VALUE statement.

        Implements EnumTypeSupport protocol.

        Note: Requires PostgreSQL 9.1+

        Args:
            type_name: Enum type name
            new_value: New value to add
            schema: Optional schema name
            before: Add before this value
            after: Add after this value

        Returns:
            SQL statement string

        """
        sql, _ = self.format_alter_enum_add_value_raw(type_name, new_value, schema, before, after)
        return sql

    # =========================================================================
    # Expression-based methods (for expression-dialect architecture)
    # =========================================================================

    def format_create_enum_type(
        self,
        expr: "PostgresCreateEnumTypeExpression",
    ) -> Tuple[str, tuple]:
        """Format CREATE TYPE ... AS ENUM from its expression node.

        Args:
            expr: :class:`PostgresCreateEnumTypeExpression` instance.

        Returns:
            Tuple of (SQL string, params tuple).

        """
        return self.format_create_enum_type_raw(
            expr.name,
            expr.values,
            expr.schema,
            expr.if_not_exists,
        )

    def format_drop_enum_type(
        self,
        expr: "PostgresDropEnumTypeExpression",
    ) -> Tuple[str, tuple]:
        """Format DROP TYPE from its expression node.

        Args:
            expr: :class:`PostgresDropEnumTypeExpression` instance.

        Returns:
            Tuple of (SQL string, params tuple).

        """
        return self.format_drop_enum_type_raw(
            expr.name,
            expr.schema,
            expr.if_exists,
            expr.cascade,
        )

    def format_alter_enum_add_value(
        self,
        expr: "PostgresAlterEnumAddValueExpression",
    ) -> Tuple[str, tuple]:
        """Format ALTER TYPE ADD VALUE from its expression node.

        Args:
            expr: :class:`PostgresAlterEnumAddValueExpression` instance.

        Returns:
            Tuple of (SQL string, params tuple).

        """
        return self.format_alter_enum_add_value_raw(
            expr.type_name,
            expr.new_value,
            expr.schema,
            expr.before,
            expr.after,
        )

    def format_enum_type_expression(self, expr) -> Tuple[str, tuple]:
        """Format a :class:`PostgresEnumType` type reference expression.

        The reference is schema-qualified when the expression carries a
        schema: ``"schema"."name"`` / ``"name"``.

        Args:
            expr: :class:`PostgresEnumType` instance.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        if expr.schema:
            return (f"{expr.schema}.{expr.name}", ())
        return (expr.name, ())

    def format_alter_enum_type_add_value(self, expr) -> Tuple[str, tuple]:
        """Format ALTER TYPE ADD VALUE statement from expression object.

        Args:
            expr: PostgresAlterEnumTypeAddValueExpression instance

        Returns:
            Tuple of (SQL string, empty params tuple)

        """
        return self.format_alter_enum_add_value_raw(
            expr.type_name,
            expr.new_value,
            expr.schema,
            expr.before,
            expr.after,
        )

    def format_alter_enum_type_rename_value(self, expr) -> Tuple[str, tuple]:
        """Format ALTER TYPE RENAME VALUE statement from expression object.

        Args:
            expr: PostgresAlterEnumTypeRenameValueExpression instance

        Returns:
            Tuple of (SQL string, empty params tuple)

        """
        full_name, _ = self.format_enum_type_name(expr.type_name, expr.schema)
        sql = f"ALTER TYPE {full_name} RENAME VALUE '{expr.old_value}' TO '{expr.new_value}'"
        return (sql, ())
