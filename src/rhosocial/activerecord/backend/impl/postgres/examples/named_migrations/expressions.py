# src/rhosocial/activerecord/backend/impl/postgres/examples/named_migrations/expressions.py
"""
DDL named expression functions for PostgreSQL migration examples.

Each function receives a *dialect* and returns a DDL expression object.
These are the building blocks used by NamedMigration up()/down() methods.
"""

from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.statements.ddl_table import (
    CreateTableExpression,
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
    DropTableExpression,
)
from rhosocial.activerecord.backend.expression.types import VarCharType
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresSerialType,
)

def _character_varying(dialect, length: int = 255) -> VarCharType:
    """A ``CHARACTER VARYING(length)`` column type.

    SQL:2016 defines ``VARCHAR`` as the abbreviation of ``CHARACTER VARYING``, so
    the two are one type written two ways — the core ``VarCharType`` with its
    ``character varying`` spelling, not a PostgreSQL-only class. Drop the
    ``spelling=`` argument for plain ``VARCHAR(length)``; both render, and both
    compare as the same concept with the same length.
    """
    return VarCharType(dialect, length=length, spelling="character varying")


def create_users_table(dialect):
    """CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))."""
    return CreateTableExpression(
        dialect,
        table=Table(dialect, 'users'),
        columns=[
            ColumnDefinition(
                dialect,
                "id",
                PostgresSerialType(dialect),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)],
            ),
            ColumnDefinition(dialect, "name", _character_varying(dialect)),
            ColumnDefinition(dialect, "email", _character_varying(dialect)),
        ],
    )


def drop_users_table(dialect):
    """DROP TABLE IF EXISTS users."""
    return DropTableExpression(dialect, table=Table(dialect, 'users'), if_exists=True)


def create_posts_table(dialect):
    """CREATE TABLE posts (id SERIAL PRIMARY KEY, title VARCHAR(255), user_id INTEGER)."""
    return CreateTableExpression(
        dialect,
        table=Table(dialect, 'posts'),
        columns=[
            ColumnDefinition(
                dialect,
                "id",
                PostgresSerialType(dialect),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)],
            ),
            ColumnDefinition(dialect, "title", _character_varying(dialect)),
            ColumnDefinition(dialect, "user_id", _character_varying(dialect)),
        ],
    )


def drop_posts_table(dialect):
    """DROP TABLE IF EXISTS posts."""
    return DropTableExpression(dialect, table=Table(dialect, 'posts'), if_exists=True)


def create_custom_table(dialect, table_name: str = "custom_table"):
    """CREATE TABLE <table_name> (id SERIAL PRIMARY KEY, value VARCHAR(255)).

    This expression accepts an extra ``table_name`` parameter, allowing
    the migration to control the target table name at runtime.
    """
    return CreateTableExpression(
        dialect,
        table=Table(dialect, table_name),
        columns=[
            ColumnDefinition(
                dialect,
                "id",
                PostgresSerialType(dialect),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)],
            ),
            ColumnDefinition(dialect, "value", _character_varying(dialect)),
        ],
    )


def drop_custom_table(dialect, table_name: str = "custom_table"):
    """DROP TABLE IF EXISTS <table_name>."""
    return DropTableExpression(dialect, table=Table(dialect, table_name), if_exists=True)