# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/type.py
"""
PostgreSQL DDL expressions: Enum and Range Type operations.

PostgreSQL Documentation:
- CREATE TYPE: https://www.postgresql.org/docs/current/sql-createtype.html
- DROP TYPE: https://www.postgresql.org/docs/current/sql-droptype.html
- ALTER TYPE: https://www.postgresql.org/docs/current/sql-altertype.html

Version Requirements:
- ENUM type: PostgreSQL 8.3+
- ADD VALUE: PostgreSQL 9.1+
- RENAME VALUE: PostgreSQL 9.1+
- RANGE type: PostgreSQL 9.2+
"""

from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    # Enum types
    "CreateEnumTypeExpression",
    "DropEnumTypeExpression",
    "AlterEnumAddValueExpression",
    # Additional
    "AlterEnumTypeAddValueExpression",
    "AlterEnumTypeRenameValueExpression",
    # Range type
    "CreateRangeTypeExpression",
]


# =============================================================================
# Enum Type Operations
# =============================================================================

class CreateEnumTypeExpression(BaseExpression):
    """PostgreSQL CREATE TYPE ... AS ENUM statement expression.

    Creates a new enumerated type consisting of a set of static string values.
    ENUM types are useful for finite sets of values like status codes.

    Attributes:
        name: Name of the enum type.
        values: List of allowed values (must be unique).
        schema: Schema name for the type.
        if_not_exists: Add IF NOT EXISTS clause.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> create_enum = CreateEnumTypeExpression(
        ...     dialect=dialect,
        ...     name="order_status",
        ...     values=["pending", "processing", "shipped", "delivered", "cancelled"],
        ... )
        >>> sql, params = create_enum.to_sql()
        >>> sql
        "CREATE TYPE order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered', 'cancelled')"

        >>> # With schema and IF NOT EXISTS
        >>> create_enum = CreateEnumTypeExpression(
        ...     dialect=dialect,
        ...     name="priority",
        ...     values=["low", "medium", "high"],
        ...     schema="app",
        ...     if_not_exists=True,
        ... )
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        values: List[str],
        schema: Optional[str] = None,
        if_not_exists: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.values = values
        self.schema = schema
        self.if_not_exists = if_not_exists
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate CREATE TYPE AS ENUM SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        return self.dialect.format_create_enum_type(self)


class DropEnumTypeExpression(BaseExpression):
    """PostgreSQL DROP TYPE statement expression for enum types.

    Drops an existing enum type from the database.

    Attributes:
        name: Name of the enum type to drop.
        schema: Schema name for the type.
        if_exists: Add IF EXISTS clause (prevent error if not exists).
        cascade: Drop dependent objects (CASCADE).

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> drop_enum = DropEnumTypeExpression(
        ...     dialect=dialect,
        ...     name="order_status",
        ... )
        >>> sql, params = drop_enum.to_sql()
        >>> sql
        "DROP TYPE order_status"

        >>> # With IF EXISTS and CASCADE
        >>> drop_enum = DropEnumTypeExpression(
        ...     dialect=dialect,
        ...     name="priority",
        ...     schema="app",
        ...     if_exists=True,
        ...     cascade=True,
        ... )
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.schema = schema
        self.if_exists = if_exists
        self.cascade = cascade
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate DROP TYPE SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        return self.dialect.format_drop_enum_type(self)


class AlterEnumAddValueExpression(BaseExpression):
    """PostgreSQL ALTER TYPE ... ADD VALUE statement expression.

    Adds a new value to an existing enum type.
    The new value can be inserted before or after an existing value.

    Attributes:
        type_name: Name of the enum type to alter.
        new_value: New value to add.
        schema: Schema name for the type.
        before: Add new value before this existing value.
        after: Add new value after this existing value.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> add_value = AlterEnumAddValueExpression(
        ...     dialect=dialect,
        ...     type_name="order_status",
        ...     new_value="returned",
        ... )
        >>> sql, params = add_value.to_sql()
        >>> sql
        "ALTER TYPE order_status ADD VALUE 'returned'"

        >>> # Add in order
        >>> add_value = AlterEnumAddValueExpression(
        ...     dialect=dialect,
        ...     type_name="order_status",
        ...     new_value="refunded",
        ...     after="cancelled",
        ... )
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        type_name: str,
        new_value: str,
        schema: Optional[str] = None,
        before: Optional[str] = None,
        after: Optional[str] = None,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.type_name = type_name
        self.new_value = new_value
        self.schema = schema
        self.before = before
        self.after = after
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate ALTER TYPE ADD VALUE SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        return self.dialect.format_alter_enum_add_value(self)


class AlterEnumTypeAddValueExpression(BaseExpression):
    """PostgreSQL ALTER TYPE ... ADD VALUE statement.

    Adds a new value to an existing enum type with validation.

    Attributes:
        type_name: Name of the enum type to alter.
        new_value: New value to add.
        schema: Schema name for the type.
        before: Add new value before this existing value.
        after: Add new value after this existing value.

    Raises:
        ValueError: If type_name or new_value is empty, or both before and after specified.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        type_name: str,
        new_value: str,
        schema: Optional[str] = None,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ):
        super().__init__(dialect)
        if not type_name:
            raise ValueError("Enum type name cannot be empty")
        if not new_value:
            raise ValueError("New value cannot be empty")
        if before and after:
            raise ValueError("Cannot specify both 'before' and 'after'")

        self.type_name = type_name
        self.new_value = new_value
        self.schema = schema
        self.before = before
        self.after = after

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate ALTER TYPE ADD VALUE SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        full_type_name = f"{self.schema}.{self.type_name}" if self.schema else self.type_name
        sql = f"ALTER TYPE {full_type_name} ADD VALUE '{self.new_value}'"

        if self.before:
            sql += f" BEFORE '{self.before}'"
        elif self.after:
            sql += f" AFTER '{self.after}'"

        return (sql, ())


class AlterEnumTypeRenameValueExpression(BaseExpression):
    """PostgreSQL ALTER TYPE ... RENAME VALUE statement.

    Renames an existing value in an enum type.

    Attributes:
        type_name: Name of the enum type to alter.
        old_value: Current value to rename.
        new_value: New name for the value.
        schema: Schema name for the type.

    Raises:
        ValueError: If type_name, old_value, or new_value is empty.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        type_name: str,
        old_value: str,
        new_value: str,
        schema: Optional[str] = None,
    ):
        super().__init__(dialect)
        if not type_name:
            raise ValueError("Enum type name cannot be empty")
        if not old_value or not new_value:
            raise ValueError("Both old and new values must be specified")

        self.type_name = type_name
        self.old_value = old_value
        self.new_value = new_value
        self.schema = schema

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate ALTER TYPE RENAME VALUE SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        full_type_name = f"{self.schema}.{self.type_name}" if self.schema else self.type_name
        sql = f"ALTER TYPE {full_type_name} RENAME VALUE '{self.old_value}' TO '{self.new_value}'"
        return (sql, ())


class CreateRangeTypeExpression(BaseExpression):
    """PostgreSQL CREATE TYPE ... AS RANGE statement expression.

    Creates a new range type for representing ranges of values.
    Useful for date ranges, numeric ranges, etc.

    Attributes:
        name: Name of the range type.
        subtype: Base type for the range (e.g., 'int4', 'int8', 'date', 'timestamp').
        schema: Schema name for the type.
        subtype_opclass: Operator class for the subtype.
        collation: Collation for the subtype.
        canonical: Canonical function for the range.
        subtype_diff: Subtype difference function.
        if_not_exists: Add IF NOT EXISTS clause.

    Raises:
        ValueError: If name or subtype is empty.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> create_range = CreateRangeTypeExpression(
        ...     dialect=dialect,
        ...     name="int4range",
        ...     subtype="int4",
        ... )
        >>> sql, params = create_range.to_sql()
        >>> sql
        "CREATE TYPE int4range AS RANGE (subtype=int4)"

        >>> # Date range with custom functions (PG 9.2+)
        >>> create_range = CreateRangeTypeExpression(
        ...     dialect=dialect,
        ...     name="date_range",
        ...     subtype="date",
        ...     collation="en_US",
        ... )
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        subtype: str,
        schema: Optional[str] = None,
        subtype_opclass: Optional[str] = None,
        collation: Optional[str] = None,
        canonical: Optional[str] = None,
        subtype_diff: Optional[str] = None,
        if_not_exists: bool = False,
    ):
        super().__init__(dialect)
        if not name:
            raise ValueError("Range type name cannot be empty")
        if not subtype:
            raise ValueError("Subtype cannot be empty")

        self.name = name
        self.subtype = subtype
        self.schema = schema
        self.subtype_opclass = subtype_opclass
        self.collation = collation
        self.canonical = canonical
        self.subtype_diff = subtype_diff
        self.if_not_exists = if_not_exists

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate CREATE TYPE AS RANGE SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        exists_clause = "IF NOT EXISTS " if self.if_not_exists else ""
        type_name = f"{self.schema}.{self.name}" if self.schema else self.name

        options = [f"subtype={self.subtype}"]

        if self.subtype_opclass:
            options.append(f"subtype_opclass={self.subtype_opclass}")
        if self.collation:
            options.append(f"collation={self.collation}")
        if self.canonical:
            options.append(f"canonical={self.canonical}")
        if self.subtype_diff:
            options.append(f"subtype_diff={self.subtype_diff}")

        options_str = ", ".join(options)
        sql = f"CREATE TYPE {exists_clause}{type_name} AS RANGE ({options_str})"
        return (sql, ())
