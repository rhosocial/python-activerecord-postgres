# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/bloom.py
"""bloom extension protocol definition.

This module defines the protocol for bloom filter index
functionality in PostgreSQL.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresBloomSupport(Protocol):
    """bloom filter index extension protocol.

    Feature Source: Extension support (requires bloom extension)

    bloom provides bloom filter index access method:
    - Memory-efficient index
    - Low false positive rate

    Extension Information:
    - Extension name: bloom
    - Install command: CREATE EXTENSION bloom;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/bloom.html
    """

    def supports_bloom_index(self) -> bool:
        """Whether bloom index is supported."""
        ...