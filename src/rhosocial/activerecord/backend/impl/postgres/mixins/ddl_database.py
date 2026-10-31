# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl_database.py
"""PostgreSQL database DDL mixin."""
from __future__ import annotations

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.objects import Database

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.statements.ddl_database import (
        AlterDatabaseStatement,
        CreateDatabaseStatement,
        DropDatabaseStatement,
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
        return self.version_at_least((13, 0, 0))

    def supports_database_if_exists(self) -> bool:
        """PostgreSQL 13+ supports IF EXISTS."""
        return self.version_at_least((13, 0, 0))

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
        self, expr: CreateDatabaseStatement
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
                f"CreateDatabaseStatement.database must be a Database, "
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
        self, expr: DropDatabaseStatement
    ) -> Tuple[str, tuple]:
        """Render ``DROP DATABASE``, with the PostgreSQL-only options.

        Raises:
            TypeError: ``expr.database`` is not a :class:`Database`.
        """
        if not isinstance(expr.database, Database):
            raise TypeError(
                f"DropDatabaseStatement.database must be a Database, "
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
        self, expr: AlterDatabaseStatement
    ) -> Tuple[str, tuple]:
        """Render ``ALTER DATABASE``.

        PostgreSQL has two actions core's vocabulary does not name:
        ``CONNECTION LIMIT`` and ``SET TABLESPACE``. They are recognised by the
        word the caller passes, because there is no member to compare against --
        the override used to compare against ``AlterDatabaseAction.CONNECTION_LIMIT``
        and ``.TABLESPACE``, neither of which core declares, so every ALTER
        DATABASE that was not a rename or an owner change died with
        ``AttributeError`` before rendering anything.

        Everything else is core's: RENAME TO, OWNER TO, SET, UNSET, CHARACTER
        SET, COLLATION, SWAP WITH and the replication/failover switches all have
        members and a formatter, and PostgreSQL spells them the standard way.

        Raises:
            TypeError: ``expr.database`` is not a :class:`Database`.
            UnsupportedFeatureError: the action is one neither core nor this
                backend knows how to spell. Rendering ``ALTER DATABASE db`` with
                nothing after it would be a statement the server rejects while
                looking like success.
        """
        if not isinstance(expr.database, Database):
            raise TypeError(
                f"AlterDatabaseStatement.database must be a Database, "
                f"got {type(expr.database).__name__}"
            )
        action = str(expr.action)
        if action == "CONNECTION LIMIT":
            return (
                f"ALTER DATABASE {expr.database.to_sql()[0]} "
                f"CONNECTION LIMIT = {expr.target}",
                (),
            )
        if action == "SET TABLESPACE":
            return (
                f"ALTER DATABASE {expr.database.to_sql()[0]} "
                f"SET TABLESPACE {self.format_identifier(expr.target)}",
                (),
            )
        from rhosocial.activerecord.backend.expression.statements.ddl_database import (
            AlterDatabaseAction,
        )

        # A read artifact carries the word, not the member (the XML codec even
        # writes the member's *name* today), so resolve what was handed to the
        # core vocabulary before handing it on.
        try:
            resolved = AlterDatabaseAction(action)
        except ValueError:
            try:
                resolved = AlterDatabaseAction[
                    action.rsplit(".", 1)[-1]
                ]
            except KeyError:
                raise UnsupportedFeatureError(
                    self.name,
                    f"ALTER DATABASE action {action!r}",
                    "this action is neither a core ALTER DATABASE action nor one "
                    "of PostgreSQL's CONNECTION LIMIT / SET TABLESPACE",
                ) from None
        if resolved is not expr.action:
            expr = type(expr)(
                expr.dialect,
                expr.database,
                resolved,
                expr.target,
                expr.properties,
                expr.if_exists,
            )
        # Named explicitly rather than through ``super()``: the next class in
        # this dialect's MRO is the ``AlterDatabaseSupport`` protocol, whose
        # ``...`` body would answer ``None`` and turn a real render into
        # ``ProtocolNotImplementedError``.
        from rhosocial.activerecord.backend.dialect.mixins.ddl_database import (
            DatabaseMixin,
        )

        return DatabaseMixin.format_alter_database_statement(self, expr)


__all__ = ['PostgresDatabaseMixin']
