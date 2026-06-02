# src/rhosocial/activerecord/backend/impl/postgres/expression/locking.py
"""
PostgreSQL-specific row-level locking expressions.

PostgreSQL supports advanced row-level locking with multiple lock strengths
beyond the standard FOR UPDATE:
- FOR NO KEY UPDATE: Weaker exclusive lock that allows FOR KEY SHARE
- FOR SHARE: Shared lock that allows other readers
- FOR KEY SHARE: Weakest shared lock

Version requirements:
- FOR UPDATE: All PostgreSQL versions
- FOR NO KEY UPDATE: PostgreSQL 9.0+
- FOR SHARE: PostgreSQL 9.0+
- FOR KEY SHARE: PostgreSQL 9.3+

Use the generic ``ForUpdateClause`` with ``dialect_options={"lock_strength": ...}``
to specify PostgreSQL lock strengths:

    >>> from rhosocial.activerecord.backend.expression.query_parts import ForUpdateClause
    >>> from rhosocial.activerecord.backend.impl.postgres.expression.locking import LockStrength
    >>> clause = ForUpdateClause(dialect, dialect_options={"lock_strength": LockStrength.SHARE})
"""

from enum import Enum


class LockStrength(Enum):
    """
    Enumeration of PostgreSQL row-level lock strength options.

    These options control the type of lock acquired on selected rows.
    Only PostgreSQL supports lock strengths beyond FOR UPDATE.

    Lock strength hierarchy (PostgreSQL):
    - UPDATE: Strongest exclusive lock, blocks all other locks
    - NO_KEY_UPDATE: Weaker exclusive lock, allows KEY_SHARE
    - SHARE: Shared lock, allows other SHARE and KEY_SHARE
    - KEY_SHARE: Weakest shared lock, allows NO_KEY_UPDATE and SHARE

    Version requirements (PostgreSQL):
    - FOR UPDATE: All versions
    - FOR NO KEY UPDATE: PostgreSQL 9.0+
    - FOR SHARE: PostgreSQL 9.0+
    - FOR KEY SHARE: PostgreSQL 9.3+
    """

    UPDATE = "FOR UPDATE"  # Exclusive lock (strongest)
    NO_KEY_UPDATE = "FOR NO KEY UPDATE"  # Weaker exclusive lock
    SHARE = "FOR SHARE"  # Shared lock
    KEY_SHARE = "FOR KEY SHARE"  # Weakest shared lock
