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
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.query_parts import ForUpdateClause
from rhosocial.activerecord.backend.expression import bases

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


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


class PostgresForUpdateClause(ForUpdateClause):
    """
    PostgreSQL-specific FOR UPDATE clause with lock strength support.

    Extends the standard ForUpdateClause with PostgreSQL's advanced
    row-level locking capabilities.

    PostgreSQL supports additional lock strengths beyond FOR UPDATE:
    - FOR NO KEY UPDATE: Weaker exclusive lock that allows FOR KEY SHARE
    - FOR SHARE: Shared lock that allows other readers
    - FOR KEY SHARE: Weakest shared lock

    Example Usage:
        # Basic FOR UPDATE (same as parent class)
        for_update = PostgresForUpdateClause(dialect)

        # FOR SHARE (PostgreSQL 9.0+)
        for_update = PostgresForUpdateClause(dialect, strength=LockStrength.SHARE)

        # FOR NO KEY UPDATE (PostgreSQL 9.0+)
        for_update = PostgresForUpdateClause(dialect, strength=LockStrength.NO_KEY_UPDATE)

        # FOR KEY SHARE (PostgreSQL 9.3+)
        for_update = PostgresForUpdateClause(dialect, strength=LockStrength.KEY_SHARE)

        # FOR SHARE with NOWAIT
        for_update = PostgresForUpdateClause(
            dialect,
            strength=LockStrength.SHARE,
            nowait=True
        )
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        strength: Optional[LockStrength] = None,
        of_columns: Optional[List[Union[str, "bases.BaseExpression"]]] = None,
        nowait: bool = False,
        skip_locked: bool = False,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize PostgreSQL FOR UPDATE clause.

        Args:
            dialect: The SQL dialect to use for formatting
            strength: Lock strength (defaults to UPDATE for backward compatibility)
            of_columns: Columns to apply the lock to
            nowait: If True, fail immediately if rows are locked
            skip_locked: If True, skip locked rows instead of waiting
            dialect_options: Additional dialect-specific options
        """
        super().__init__(
            dialect,
            of_columns=of_columns,
            nowait=nowait,
            skip_locked=skip_locked,
            dialect_options=dialect_options,
        )
        # Default to UPDATE for backward compatibility
        self.strength = strength if strength is not None else LockStrength.UPDATE

    def to_sql(self) -> "bases.SQLQueryAndParams":
        """
        Generate the SQL representation of the PostgreSQL FOR UPDATE clause.

        Delegates to the dialect's format_postgres_for_update_clause method
        to follow the Expression-Dialect separation pattern.

        Returns:
            Tuple containing:
            - SQL string fragment for the FOR UPDATE clause
            - Tuple of parameter values for prepared statements
        """
        return self.dialect.format_postgres_for_update_clause(self)