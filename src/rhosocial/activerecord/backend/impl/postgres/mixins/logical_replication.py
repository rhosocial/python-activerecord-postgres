# src/rhosocial/activerecord/backend/impl/postgres/mixins/logical_replication.py
"""PostgreSQL logical replication enhancements implementation."""
from typing import Any, Dict, Optional, Tuple, List


class PostgresLogicalReplicationMixin:
    """PostgreSQL logical replication enhancements implementation.

    All features are native, using version number for detection.
    """

    def supports_commit_timestamp(self) -> bool:
        """Commit timestamp is native feature, PG 10+."""
        return self.version >= (10, 0, 0)

    def supports_streaming_transactions(self) -> bool:
        """Streaming transactions is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def supports_two_phase_decoding(self) -> bool:
        """Two-phase decoding is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def supports_binary_replication(self) -> bool:
        """Binary replication is native feature, PG 14+."""
        return self.version >= (14, 0, 0)
