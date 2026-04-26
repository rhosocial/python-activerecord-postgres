# src/rhosocial/activerecord/backend/impl/postgres/mixins/dml/locking.py
"""PostgreSQL row-level locking implementation.

This module provides the PostgresLockingMixin class for generating
PostgreSQL-specific FOR UPDATE clauses with lock strength support.
"""

from typing import Tuple


class PostgresLockingMixin:
    """PostgreSQL row-level locking implementation.

    All features are native, using version number for detection.
    """

    def supports_for_no_key_update(self) -> bool:
        """FOR NO KEY UPDATE is native feature, PG 9.0+."""
        return self.version >= (9, 0, 0)

    def supports_for_share(self) -> bool:
        """FOR SHARE is native feature, PG 9.0+."""
        return self.version >= (9, 0, 0)

    def supports_for_key_share(self) -> bool:
        """FOR KEY SHARE is native feature, PG 9.3+."""
        return self.version >= (9, 3, 0)

    def supports_for_update_skip_locked(self) -> bool:
        """FOR UPDATE SKIP LOCKED is native feature, PG 9.5+."""
        return self.version >= (9, 5, 0)

    def format_for_update_clause(self, clause) -> Tuple[str, tuple]:
        """Format PostgreSQL-specific FOR UPDATE clause.

        Handles PostgreSQL-specific lock strengths (FOR NO KEY UPDATE,
        FOR SHARE, FOR KEY SHARE) and options (NOWAIT, SKIP LOCKED).

        Args:
            clause: PostgresForUpdateClause instance

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
        from rhosocial.activerecord.backend.impl.postgres.expression.locking import LockStrength

        all_params = []

        # Handle both base ForUpdateClause (no strength) and PostgresForUpdateClause
        strength = getattr(clause, 'strength', LockStrength.UPDATE)

        # Check version support for lock strength
        if strength == LockStrength.NO_KEY_UPDATE:
            if not self.supports_for_no_key_update():
                raise UnsupportedFeatureError(
                    self.name, "FOR NO KEY UPDATE (requires PostgreSQL 9.0+)"
                )
        elif strength == LockStrength.SHARE:
            if not self.supports_for_share():
                raise UnsupportedFeatureError(
                    self.name, "FOR SHARE (requires PostgreSQL 9.0+)"
                )
        elif strength == LockStrength.KEY_SHARE:
            if not self.supports_for_key_share():
                raise UnsupportedFeatureError(
                    self.name, "FOR KEY SHARE (requires PostgreSQL 9.3+)"
                )

        # Use the strength value directly (e.g., "FOR UPDATE", "FOR SHARE")
        sql_parts = [strength.value]

        # Handle OF columns if specified
        if clause.of_columns:
            of_parts = []
            for col in clause.of_columns:
                if isinstance(col, str):
                    of_parts.append(self.format_identifier(col))
                else:
                    # BaseExpression
                    col_sql, col_params = col.to_sql()
                    of_parts.append(col_sql)
                    all_params.extend(col_params)
            if of_parts:
                sql_parts.append(f"OF {', '.join(of_parts)}")

        # Handle NOWAIT/SKIP LOCKED options
        if clause.nowait:
            sql_parts.append("NOWAIT")
        elif clause.skip_locked:
            if not self.supports_for_update_skip_locked():
                raise UnsupportedFeatureError(
                    self.name, "SKIP LOCKED (requires PostgreSQL 9.5+)"
                )
            sql_parts.append("SKIP LOCKED")

        return " ".join(sql_parts), tuple(all_params)
