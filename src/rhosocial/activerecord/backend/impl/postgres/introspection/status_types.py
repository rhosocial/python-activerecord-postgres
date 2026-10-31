# src/rhosocial/activerecord/backend/impl/postgres/introspection/status_types.py
"""PostgreSQL's own server-status families, brought home from core.

Core keeps the smallest shape an ActiveRecord layer generally needs, and
``ServerOverview`` is that shape: the public slots every engine can answer. The
four families below are not that -- a WAL is PostgreSQL's write mechanism, its
archiver and its hot-standby protocol answer questions no other engine asks --
so they used to sit in core asserting that every backend had them. They now
live beside the introspector that fills them
(``...introspection.status_introspector``), and an overview that answers them is
the subclass declared at the bottom of this module.

The types themselves are unchanged from where core carried them: the move is a
change of address, not of shape, so a caller that read ``wal.wal_level`` before
reads the same field now.

Where the subclass fits: ``get_overview()``'s core signature does not change.
A ``PostgreSQLServerOverview`` *is* a ``ServerOverview``, so returning one from
the method core declares is a legal covariant return, and ``to_dict()``
iterates the instance's fields instead of naming them, so the four facts this
subclass adds serialize without being re-declared anywhere. The LSP shape is
therefore: derive, add the typed slots, and the cross-dialect entry point keeps
working untouched (`.claude/rules/introspection.md` §5: one question, drifted
fields -- core holds the common minimum and each backend keeps its own complete
class beside it).
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from rhosocial.activerecord.backend.introspection.status import ServerOverview

__all__ = [
    "WALInfo",
    "ReplicationInfo",
    "ArchiveInfo",
    "SecurityInfo",
    "PostgreSQLServerOverview",
]


@dataclass
class WALInfo:
    """Write-Ahead Logging information.

    Attributes:
        wal_level: WAL level (minimal, replica, logical)
        wal_size_bytes: Current WAL size in bytes
        wal_files: Number of WAL files
        checkpoint_count: Number of checkpoints
        checkpoint_time: Time of last checkpoint
        wal_write_rate: WAL write rate in bytes/second
        wal_segments: Current number of WAL segments
        extra: Additional backend-specific information
    """

    wal_level: Optional[str] = None
    wal_size_bytes: Optional[int] = None
    wal_files: Optional[int] = None
    checkpoint_count: Optional[int] = None
    checkpoint_time: Optional[str] = None
    wal_write_rate: Optional[float] = None
    wal_segments: Optional[int] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ReplicationInfo:
    """Replication status information.

    Attributes:
        is_primary: Whether this server is a primary/master
        is_standby: Whether this server is a standby/replica
        replication_slots: Number of replication slots
        active_replications: Number of active replication connections
        lag_bytes: Replication lag in bytes (for standby)
        streaming: Whether streaming replication is active
        extra: Additional backend-specific information
    """

    is_primary: Optional[bool] = None
    is_standby: Optional[bool] = None
    replication_slots: Optional[int] = None
    active_replications: Optional[int] = None
    lag_bytes: Optional[int] = None
    streaming: Optional[bool] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ArchiveInfo:
    """Archive status information.

    Attributes:
        archive_mode: Archive mode (on, off, always)
        archive_command: Archive command
        archive_timeout: Archive timeout in seconds
        archived_count: Number of archived WAL files
        failed_count: Number of failed archive attempts
        last_archived: Time of last archive
        last_failed: Time of last failed archive
        extra: Additional backend-specific information
    """

    archive_mode: Optional[str] = None
    archive_command: Optional[str] = None
    archive_timeout: Optional[int] = None
    archived_count: Optional[int] = None
    failed_count: Optional[int] = None
    last_archived: Optional[str] = None
    last_failed: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SecurityInfo:
    """Security-related status information.

    Attributes:
        ssl_enabled: Whether SSL is enabled
        ssl_cert_file: SSL certificate file path
        ssl_key_file: SSL key file path
        ssl_ca_file: SSL CA file path
        password_encryption: Password encryption method
        auth_method: Authentication method
        krb_enabled: Whether Kerberos is enabled
        row_security: Row-level security status
        extra: Additional backend-specific information
    """

    ssl_enabled: Optional[bool] = None
    ssl_cert_file: Optional[str] = None
    ssl_key_file: Optional[str] = None
    ssl_ca_file: Optional[str] = None
    password_encryption: Optional[str] = None
    auth_method: Optional[str] = None
    krb_enabled: Optional[bool] = None
    row_security: Optional[bool] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PostgreSQLServerOverview(ServerOverview):
    """A complete PostgreSQL server status overview.

    The four typed slots are PostgreSQL's own answers to its own questions:
    the WAL's state, the standby protocol, the archiver and the server's
    security posture. They are named fields rather than ``extra`` entries
    because a fact only one engine has is a typed field on that engine's class,
    and because ``extra`` is the channel for a spelling that maps onto a core
    member, never for a second family's fields.

    **Why this is a subclass and not a widened core type.** Core's
    ``ServerOverview`` stays the cross-dialect entry point: it keeps the
    public slots and declares nothing an engine cannot answer. This class
    derives from it and adds what PostgreSQL can, so:

    * ``get_overview()``'s signature is unchanged -- the subclass is a legal
      covariant return, and ``isinstance(overview, ServerOverview)`` still
      holds for every caller written against core;
    * ``to_dict()`` is field-driven, so ``wal``, ``replication``, ``archive``
      and ``security`` serialize without being re-declared here, exactly as
      the fields inherited from core do.

    Every slot defaults to ``None``, so an overview built by a query that could
    not answer one of the four families still describes the server honestly
    instead of asserting a fact it does not have.
    """

    wal: Optional[WALInfo] = None
    replication: Optional[ReplicationInfo] = None
    archive: Optional[ArchiveInfo] = None
    security: Optional[SecurityInfo] = None
