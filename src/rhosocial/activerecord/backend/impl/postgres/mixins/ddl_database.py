# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl_database.py
"""PostgreSQL database DDL mixin."""
from __future__ import annotations

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.objects import Database

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.statements.ddl_database import (
        AlterDatabaseExpression,
        CreateDatabaseExpression,
        DropDatabaseExpression,
    )


class PostgresDatabaseMixin:
    """PostgreSQL database DDL support.

    PostgreSQL has rich database support including OWNER, ENCODING,
    TABLESPACE, TEMPLATE, CONNECTION LIMIT, and WITH (FORCE) drop.
    """

    def supports_database(self) -> bool:
        return True

    def supports_create_database(self) -> bool:
        return True

    def supports_drop_database(self) -> bool:
        return True

    def supports_alter_database(self) -> bool:
        return True

    def supports_database_if_not_exists(self) -> bool:
        """PostgreSQL 13+ supports IF NOT EXISTS."""
        return self.version >= (13, 0, 0)

    def supports_database_if_exists(self) -> bool:
        """PostgreSQL 13+ supports IF EXISTS."""
        return self.version >= (13, 0, 0)

    def supports_database_owner(self) -> bool:
        """PostgreSQL supports OWNER TO."""
        return True

    def supports_database_encoding(self) -> bool:
        """PostgreSQL supports ENCODING (immutable after creation)."""
        return True

    def supports_database_collation(self) -> bool:
        """PostgreSQL supports LC_COLLATE."""
        return True

    def supports_database_template(self) -> bool:
        """PostgreSQL supports TEMPLATE."""
        return True

    def supports_database_tablespace(self) -> bool:
        """PostgreSQL supports TABLESPACE."""
        return True

    def supports_database_connection_limit(self) -> bool:
        """PostgreSQL supports CONNECTION LIMIT."""
        return True

    def supports_database_force_drop(self) -> bool:
        """PostgreSQL supports DROP DATABASE WITH (FORCE)."""
        return True

    def format_create_database_statement(
        self, expr: CreateDatabaseExpression
    ) -> Tuple[str, tuple]:
        """Render ``CREATE DATABASE``, with the PostgreSQL-only options.

        Overrides core's version to add ENCODING / LC_COLLATE / TEMPLATE /
        OWNER / CONNECTION LIMIT / TABLESPACE. Overriding means overriding the
        kind check too: this body is the one that runs, so the check core's body
        carries is not reached. Without it, a statement handed a ``Table`` renders
        ``CREATE DATABASE users`` -- well-formed, and naming a table.

        Raises:
            TypeError: ``expr.database`` is not a :class:`Database`.
        """
        if not isinstance(expr.database, Database):
            raise TypeError(
                f"CreateDatabaseExpression.database must be a Database, "
                f"got {type(expr.database).__name__}"
            )
        parts = ["CREATE DATABASE"]
        parts.append(expr.database.to_sql()[0])
        if expr.encoding:
            parts.append(f"ENCODING = '{expr.encoding}'")
        if expr.collation:
            parts.append(f"LC_COLLATE = '{expr.collation}'")
        if expr.template:
            parts.append(f"TEMPLATE = {self.format_identifier(expr.template)}")
        if expr.owner:
            parts.append(f"OWNER = {self.format_identifier(expr.owner)}")
        if expr.connection_limit is not None:
            parts.append(f"CONNECTION LIMIT = {expr.connection_limit}")
        if expr.tablespace:
            parts.append(f"TABLESPACE = {self.format_identifier(expr.tablespace)}")
        return " ".join(parts), ()

    def format_drop_database_statement(
        self, expr: DropDatabaseExpression
    ) -> Tuple[str, tuple]:
        """Render ``DROP DATABASE``, with the PostgreSQL-only options.

        Raises:
            TypeError: ``expr.database`` is not a :class:`Database`.
        """
        if not isinstance(expr.database, Database):
            raise TypeError(
                f"DropDatabaseExpression.database must be a Database, "
                f"got {type(expr.database).__name__}"
            )
        parts = ["DROP DATABASE"]
        if expr.if_exists and self.supports_database_if_exists():
            parts.append("IF EXISTS")
        parts.append(expr.database.to_sql()[0])
        if expr.force and self.supports_database_force_drop():
            parts.append("WITH (FORCE)")
        return " ".join(parts), ()

    def format_alter_database_statement(
        self, expr: AlterDatabaseExpression
    ) -> Tuple[str, tuple]:
        """Render ``ALTER DATABASE``.

        Raises:
            TypeError: ``expr.database`` is not a :class:`Database`.
        """
        if not isinstance(expr.database, Database):
            raise TypeError(
                f"AlterDatabaseExpression.database must be a Database, "
                f"got {type(expr.database).__name__}"
            )
        from rhosocial.activerecord.backend.expression.statements.ddl_database import AlterDatabaseAction
        parts = ["ALTER DATABASE"]
        parts.append(expr.database.to_sql()[0])
        if expr.action == AlterDatabaseAction.RENAME_TO:
            parts.append(f"RENAME TO {self.format_identifier(expr.target)}")
        elif expr.action == AlterDatabaseAction.OWNER_TO:
            parts.append(f"OWNER TO {self.format_identifier(expr.target)}")
        elif expr.action == AlterDatabaseAction.CONNECTION_LIMIT:
            parts.append(f"CONNECTION LIMIT = {expr.target}")
        elif expr.action == AlterDatabaseAction.TABLESPACE:
            parts.append(f"SET TABLESPACE {self.format_identifier(expr.target)}")
        return " ".join(parts), ()


__all__ = ['PostgresDatabaseMixin']
