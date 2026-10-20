# src/rhosocial/activerecord/backend/impl/postgres/expression/status.py
"""PostgreSQL status-introspection expressions.

The status introspector used to hold the catalog SQL itself -- f-strings
assembled at the call site, or plain string literals inline in the method. A
SQL string is a formatting artefact, so it belongs to the dialect formatter,
not to the layer that executes the query (expression-dialect §16.2 of the
org's conformance guide): the node collects the parameters, the formatter
writes the text, and the introspector only constructs and executes.

Each node therefore carries only the parameters its query needs. Where the old
text bound nothing, the node binds nothing; where it carried literal
comparison values inline, the node takes them as bound values instead -- a
`WHERE name IN (...)` list is data, and a data position is a bind position.
"""

from typing import List, Optional, Sequence, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "PostgresSettingsQuery",
    "PostgresActivityCountQuery",
    "PostgresShowStatement",
    "PostgresDatabaseListQuery",
    "PostgresSchemaStatsQuery",
    "PostgresCurrentDatabaseQuery",
    "PostgresRoleListQuery",
]


class PostgresSettingsQuery(BaseExpression):
    """Read rows from ``pg_settings``.

    Without *names* every setting is read, ordered by name; with *names* only
    those settings are read and the names travel as bind values.

    Example:
        >>> expr = PostgresSettingsQuery(dialect)            # every setting
        >>> expr = PostgresSettingsQuery(dialect, ["work_mem"])   # one setting
        >>> sql, params = expr.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        names: Optional[Sequence[str]] = None,
    ):
        super().__init__(dialect)
        self.names: Optional[List[str]] = list(names) if names is not None else None

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_settings_query"


class PostgresActivityCountQuery(BaseExpression):
    """Count rows in ``pg_stat_activity``."""

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_activity_count_query"


class PostgresShowStatement(BaseExpression):
    """``SHOW`` a PostgreSQL run-time parameter.

    The parameter is a configuration name, not free text: an identifier-shaped
    string, validated at construction so a name carrying quotes or spaces fails
    at the caller instead of reaching the server as part of a statement.
    """

    def __init__(self, dialect: "SQLDialectBase", parameter: str):
        super().__init__(dialect)
        if not parameter or not parameter.replace("_", "").isalnum():
            raise ValueError(
                f"PostgresShowStatement takes a configuration name, got {parameter!r}"
            )
        self.parameter = parameter

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_show_statement"


class PostgresDatabaseListQuery(BaseExpression):
    """List databases with their sizes (non-templates plus the current one)."""

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_database_list_query"


class PostgresSchemaStatsQuery(BaseExpression):
    """Per-schema table and view counts over ``pg_namespace``/``pg_class``."""

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_schema_stats_query"


class PostgresCurrentDatabaseQuery(BaseExpression):
    """The current database's name."""

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_current_database_query"


class PostgresRoleListQuery(BaseExpression):
    """List roles with superuser flag and owner."""

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_role_list_query"
