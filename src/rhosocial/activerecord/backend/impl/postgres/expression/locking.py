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

        This method handles PostgreSQL-specific lock strengths (FOR SHARE,
        FOR NO KEY UPDATE, FOR KEY SHARE) in addition to the standard
        FOR UPDATE clause.

        Returns:
            Tuple containing:
            - SQL string fragment for the FOR UPDATE clause
            - Tuple of parameter values for prepared statements
        """
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

        all_params = []

        # Check version support for lock strength
        if self.strength == LockStrength.NO_KEY_UPDATE:
            if not self.dialect.supports_for_no_key_update():
                raise UnsupportedFeatureError(
                    self.dialect.name, "FOR NO KEY UPDATE (requires PostgreSQL 9.0+)"
                )
        elif self.strength == LockStrength.SHARE:
            if not self.dialect.supports_for_share():
                raise UnsupportedFeatureError(
                    self.dialect.name, "FOR SHARE (requires PostgreSQL 9.0+)"
                )
        elif self.strength == LockStrength.KEY_SHARE:
            if not self.dialect.supports_for_key_share():
                raise UnsupportedFeatureError(
                    self.dialect.name, "FOR KEY SHARE (requires PostgreSQL 9.3+)"
                )

        # Use the strength value directly (e.g., "FOR UPDATE", "FOR SHARE")
        sql_parts = [self.strength.value]

        # Handle OF columns if specified
        if self.of_columns:
            of_parts = []
            for col in self.of_columns:
                if isinstance(col, str):
                    of_parts.append(self.dialect.format_identifier(col))
                else:
                    # BaseExpression
                    col_sql, col_params = col.to_sql()
                    of_parts.append(col_sql)
                    all_params.extend(col_params)
            if of_parts:
                sql_parts.append(f"OF {', '.join(of_parts)}")

        # Handle NOWAIT/SKIP LOCKED options
        if self.nowait:
            sql_parts.append("NOWAIT")
        elif self.skip_locked:
            if not self.dialect.supports_for_update_skip_locked():
                raise UnsupportedFeatureError(
                    self.dialect.name, "SKIP LOCKED (requires PostgreSQL 9.5+)"
                )
            sql_parts.append("SKIP LOCKED")

        return " ".join(sql_parts), tuple(all_params)