# src/rhosocial/activerecord/backend/impl/postgres/protocols/logical_replication.py
"""PostgreSQL logical replication protocol definitions.

This module defines protocols for PostgreSQL logical replication features,
including commit timestamp tracking and streaming replication capabilities.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresLogicalReplicationSupport(Protocol):
    """PostgreSQL logical replication enhancements protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL logical replication features:
    - Commit timestamp tracking (PG 10+)
    - Streaming transactions (PG 14+)
    - Two-phase decoding (PG 14+)
    - Binary replication (PG 14+)

    Official Documentation:
    - Logical Replication: https://www.postgresql.org/docs/current/logical-replication.html
    - Commit Timestamp: https://www.postgresql.org/docs/current/functions-info.html#FUNCTIONS-INFO-COMMIT-TIMESTAMP
    - Streaming Replication: https://www.postgresql.org/docs/current/protocol-replication.html

    Version Requirements:
    - Commit timestamp: PostgreSQL 10+
    - Streaming transactions: PostgreSQL 14+
    - Two-phase decoding: PostgreSQL 14+
    - Binary replication: PostgreSQL 14+
    """

    def supports_commit_timestamp(self) -> bool:
        """Whether commit timestamp tracking is supported.

        Native feature, PostgreSQL 10+.
        Enables tracking transaction commit timestamps.
        """
        ...

    def supports_streaming_transactions(self) -> bool:
        """Whether streaming in-progress transactions is supported.

        Native feature, PostgreSQL 14+.
        Enables streaming large transactions before commit.
        """
        ...

    def supports_two_phase_decoding(self) -> bool:
        """Whether two-phase commit decoding is supported.

        Native feature, PostgreSQL 14+.
        Enables decoding of prepared transactions.
        """
        ...

    def supports_binary_replication(self) -> bool:
        """Whether binary transfer mode for replication is supported.

        Native feature, PostgreSQL 14+.
        Enables binary data transfer in logical replication.
        """
        ...
