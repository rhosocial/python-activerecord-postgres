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
    "PostgresCurrentUserQuery",
    "PostgresCurrentSchemaQuery",
    "PostgresClientAddressQuery",
    "PostgresSslSessionQuery",
    "PostgresWalPositionQuery",
    "PostgresWalFileCountQuery",
    "PostgresCheckpointCountQuery",
    "PostgresCheckpointLsnQuery",
    "PostgresRecoveryStateQuery",
    "PostgresReplicationSlotCountQuery",
    "PostgresActiveReplicationCountQuery",
    "PostgresReplicationLagQuery",
    "PostgresArchiveStatsQuery",
    "PostgresExtensionListQuery",
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


# ---------------------------------------------------------------------------
# The session / WAL / replication / archive reads
# ---------------------------------------------------------------------------
# These are the remaining questions the status introspector used to spell as
# text at the call site. Each is one row of engine facts, so each is a node with
# no parameters: what varies between PostgreSQL versions is the *statement*, and
# that belongs to the formatter (``PostgresWalPositionQuery`` is refused on a
# server older than 10, whose spelling was ``xlog``).
#
# A node rather than a shared "run this SQL" helper because the shapes differ:
# one asks a niladic function, one absorbs a set-returning function, one counts,
# one reads a catalog view. The node names which question is being asked, and
# the formatter writes whichever spelling this version needs.


class PostgresCurrentUserQuery(BaseExpression):
    """The session's current role: ``CURRENT_USER`` (niladic, no parentheses)."""

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_current_user_query"


class PostgresCurrentSchemaQuery(BaseExpression):
    """The session's current schema: ``current_schema()``."""

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_current_schema_query"


class PostgresClientAddressQuery(BaseExpression):
    """The client's address as the server sees it: ``inet_client_addr()``.

    Null over a Unix-domain socket, which the caller treats as "no host".
    """

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_client_address_query"


class PostgresSslSessionQuery(BaseExpression):
    """This session's SSL state from ``pg_stat_ssl``.

    The row is the session's own, so the view is narrowed by
    ``pg_backend_pid()`` -- the question is "am I encrypted", not "is anything".
    """

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_ssl_session_query"


class PostgresWalPositionQuery(BaseExpression):
    """How many bytes of WAL have been written: the LSN distance from ``0/0``.

    The function pair is what the server version decides: PostgreSQL 10 renamed
    ``xlog`` to ``wal``, and a 9.x server has no ``pg_current_wal_lsn`` at all,
    so the formatter asks this node's dialect which spelling it has.
    """

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_wal_position_query"


class PostgresWalFileCountQuery(BaseExpression):
    """How many files sit in the WAL directory (``pg_ls_waldir()``, 10+)."""

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_wal_file_count_query"


class PostgresCheckpointCountQuery(BaseExpression):
    """How many checkpoints have run, timed and requested together.

    The two counters live in ``pg_stat_bgwriter`` before 17 and in
    ``pg_stat_checkpointer`` from 17 on, so the relation is the version's.
    """

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_checkpoint_count_query"


class PostgresCheckpointLsnQuery(BaseExpression):
    """The last checkpoint's LSN, from ``pg_control_checkpoint()`` (9.6+)."""

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_checkpoint_lsn_query"


class PostgresRecoveryStateQuery(BaseExpression):
    """Whether this server is a standby: ``pg_is_in_recovery()``."""

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_recovery_state_query"


class PostgresReplicationSlotCountQuery(BaseExpression):
    """How many replication slots exist."""

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_replication_slot_count_query"


class PostgresActiveReplicationCountQuery(BaseExpression):
    """How many standbys are streaming right now."""

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_active_replication_count_query"


class PostgresReplicationLagQuery(BaseExpression):
    """How far behind the replay is: the receive/replay LSN distance, in bytes."""

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_replication_lag_query"


class PostgresArchiveStatsQuery(BaseExpression):
    """The archiver's counters and its last success/failure times."""

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_archive_stats_query"


class PostgresExtensionListQuery(BaseExpression):
    """Every installed extension, with its schema and description."""

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_extension_list_query"
