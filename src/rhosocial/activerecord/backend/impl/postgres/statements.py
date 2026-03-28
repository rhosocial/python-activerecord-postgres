# src/rhosocial/activerecord/backend/impl/postgres/statements.py
"""PostgreSQL-specific DDL (Data Definition Language) statements.

These expression classes are responsible for collecting the parameters and structure
for PostgreSQL-specific SQL statements and delegating the actual SQL string generation
to the PostgreSQL dialect.

This follows the expression-dialect separation architecture from the main package.
"""

from typing import List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


# region ENUM Type DDL Statements


class CreateEnumTypeExpression(BaseExpression):
    """PostgreSQL CREATE TYPE ... AS ENUM statement.

    This expression represents the creation of a PostgreSQL ENUM type.
    ENUM types in PostgreSQL are reusable custom types created with CREATE TYPE.

    Examples:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> create_enum = CreateEnumTypeExpression(
        ...     dialect=dialect,
        ...     name='status',
        ...     values=['pending', 'processing', 'ready', 'failed']
        ... )
        >>> create_enum.to_sql()
        ("CREATE TYPE status AS ENUM ('pending', 'processing', 'ready', 'failed')", ())

        >>> # With schema
        >>> create_enum = CreateEnumTypeExpression(
        ...     dialect=dialect,
        ...     name='status',
        ...     values=['draft', 'published'],
        ...     schema='app',
        ...     if_not_exists=True
        ... )
        >>> create_enum.to_sql()
        ("CREATE TYPE IF NOT EXISTS app.status AS ENUM ('draft', 'published')", ())
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        values: List[str],
        schema: Optional[str] = None,
        if_not_exists: bool = False,
    ):
        """Initialize CREATE TYPE expression.

        Args:
            dialect: SQL dialect instance
            name: ENUM type name
            values: List of allowed values
            schema: Optional schema name
            if_not_exists: Add IF NOT EXISTS clause (PostgreSQL 9.1+)

        Raises:
            ValueError: If name is empty or values is empty
        """
        super().__init__(dialect)

        if not name:
            raise ValueError("Enum type name cannot be empty")
        if not values:
            raise ValueError("ENUM must have at least one value")
        if len(values) != len(set(values)):
            raise ValueError("ENUM values must be unique")

        self.name = name
        self.values = list(values)
        self.schema = schema
        self.if_not_exists = if_not_exists

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate CREATE TYPE SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple)
        """
        values_str = ", ".join(f"'{v}'" for v in self.values)
        exists_clause = "IF NOT EXISTS " if self.if_not_exists else ""
        type_name = f"{self.schema}.{self.name}" if self.schema else self.name
        sql = f"CREATE TYPE {exists_clause}{type_name} AS ENUM ({values_str})"
        return (sql, ())


class DropEnumTypeExpression(BaseExpression):
    """PostgreSQL DROP TYPE statement for ENUM types.

    This expression represents dropping an ENUM type from the database.

    Examples:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> drop_enum = DropEnumTypeExpression(
        ...     dialect=dialect,
        ...     name='status'
        ... )
        >>> drop_enum.to_sql()
        ('DROP TYPE status', ())

        >>> # With options
        >>> drop_enum = DropEnumTypeExpression(
        ...     dialect=dialect,
        ...     name='status',
        ...     schema='app',
        ...     if_exists=True,
        ...     cascade=True
        ... )
        >>> drop_enum.to_sql()
        ('DROP TYPE IF EXISTS app.status CASCADE', ())
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False,
    ):
        """Initialize DROP TYPE expression.

        Args:
            dialect: SQL dialect instance
            name: ENUM type name
            schema: Optional schema name
            if_exists: Add IF EXISTS clause
            cascade: Add CASCADE clause

        Raises:
            ValueError: If name is empty
        """
        super().__init__(dialect)

        if not name:
            raise ValueError("Enum type name cannot be empty")

        self.name = name
        self.schema = schema
        self.if_exists = if_exists
        self.cascade = cascade

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate DROP TYPE SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple)
        """
        exists_clause = "IF EXISTS " if self.if_exists else ""
        cascade_clause = " CASCADE" if self.cascade else ""
        type_name = f"{self.schema}.{self.name}" if self.schema else self.name
        sql = f"DROP TYPE {exists_clause}{type_name}{cascade_clause}"
        return (sql, ())


class AlterEnumTypeAddValueExpression(BaseExpression):
    """PostgreSQL ALTER TYPE ... ADD VALUE statement.

    This expression represents adding a new value to an existing ENUM type.

    Note: Requires PostgreSQL 9.1+

    Examples:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> add_value = AlterEnumTypeAddValueExpression(
        ...     dialect=dialect,
        ...     type_name='status',
        ...     new_value='cancelled'
        ... )
        >>> add_value.to_sql()
        ("ALTER TYPE status ADD VALUE 'cancelled'", ())

        >>> # With position
        >>> add_value = AlterEnumTypeAddValueExpression(
        ...     dialect=dialect,
        ...     type_name='status',
        ...     new_value='cancelled',
        ...     before='pending'
        ... )
        >>> add_value.to_sql()
        ("ALTER TYPE status ADD VALUE 'cancelled' BEFORE 'pending'", ())
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
        """Initialize ALTER TYPE ADD VALUE expression.

        Args:
            dialect: SQL dialect instance
            type_name: ENUM type name
            new_value: New value to add
            schema: Optional schema name
            before: Add before this value (optional)
            after: Add after this value (optional)

        Raises:
            ValueError: If type_name or new_value is empty, or both before and after specified
        """
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
            Tuple of (SQL string, empty params tuple)
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

    This expression represents renaming a value in an existing ENUM type.

    Note: Requires PostgreSQL 9.1+

    Examples:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> rename_value = AlterEnumTypeRenameValueExpression(
        ...     dialect=dialect,
        ...     type_name='status',
        ...     old_value='pending',
        ...     new_value='waiting'
        ... )
        >>> rename_value.to_sql()
        ("ALTER TYPE status RENAME VALUE 'pending' TO 'waiting'", ())
    """

    def __init__(
        self, dialect: "SQLDialectBase", type_name: str, old_value: str, new_value: str, schema: Optional[str] = None
    ):
        """Initialize ALTER TYPE RENAME VALUE expression.

        Args:
            dialect: SQL dialect instance
            type_name: ENUM type name
            old_value: Current value name
            new_value: New value name
            schema: Optional schema name

        Raises:
            ValueError: If any required string parameter is empty
        """
        super().__init__(dialect)

        if not type_name:
            raise ValueError("Enum type name cannot be empty")
        if not old_value:
            raise ValueError("Old value cannot be empty")
        if not new_value:
            raise ValueError("New value cannot be empty")

        self.type_name = type_name
        self.old_value = old_value
        self.new_value = new_value
        self.schema = schema

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate ALTER TYPE RENAME VALUE SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple)
        """
        full_type_name = f"{self.schema}.{self.type_name}" if self.schema else self.type_name
        sql = f"ALTER TYPE {full_type_name} RENAME VALUE '{self.old_value}' TO '{self.new_value}'"
        return (sql, ())


# endregion ENUM Type DDL Statements


# region Range Type DDL Statements


class CreateRangeTypeExpression(BaseExpression):
    """PostgreSQL CREATE TYPE ... AS RANGE statement.

    This expression represents the creation of a PostgreSQL RANGE type.

    Examples:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> create_range = CreateRangeTypeExpression(
        ...     dialect=dialect,
        ...     name='float_range',
        ...     subtype='float8'
        ... )
        >>> create_range.to_sql()
        ('CREATE TYPE float_range AS RANGE (subtype=float8)', ())
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
        """Initialize CREATE RANGE TYPE expression.

        Args:
            dialect: SQL dialect instance
            name: Range type name
            subtype: Subtype (base type) for the range
            schema: Optional schema name
            subtype_opclass: Optional subtype operator class
            collation: Optional collation for the subtype
            canonical: Optional canonical function
            subtype_diff: Optional subtype difference function
            if_not_exists: Add IF NOT EXISTS clause (PostgreSQL 9.1+)

        Raises:
            ValueError: If name or subtype is empty
        """
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
            Tuple of (SQL string, empty params tuple)
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


# endregion Range Type DDL Statements
