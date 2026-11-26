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

    def supports_lock_strength_for_update(self) -> bool:
        """FOR UPDATE is the original row lock, so it has no version gate."""
        return True

    def supports_lock_strength_no_key_update(self) -> bool:
        """FOR NO KEY UPDATE arrived with the lock-strength vocabulary in 9.0."""
        return self.version_at_least((9, 0, 0))

    def supports_lock_strength_share(self) -> bool:
        """FOR SHARE's current spelling arrived with the vocabulary in 9.0."""
        return self.version_at_least((9, 0, 0))

    def supports_lock_strength_key_share(self) -> bool:
        """FOR KEY SHARE arrived in PostgreSQL 9.3."""
        return self.version_at_least((9, 3, 0))

    def supports_for_no_key_update(self) -> bool:
        """FOR NO KEY UPDATE is native feature, PG 9.0+."""
        return self.supports_lock_strength_no_key_update()

    def supports_for_share(self) -> bool:
        """FOR SHARE is native feature, PG 9.0+."""
        return self.supports_lock_strength_share()

    def supports_for_key_share(self) -> bool:
        """FOR KEY SHARE is native feature, PG 9.3+."""
        return self.supports_lock_strength_key_share()

    def supports_for_update_nowait(self) -> bool:
        """FOR UPDATE NOWAIT is native on every supported version."""
        return True

    def supports_for_update_skip_locked(self) -> bool:
        """FOR UPDATE SKIP LOCKED is native feature, PG 9.5+."""
        return self.version_at_least((9, 5, 0))

    def format_for_update_clause(self, expr) -> Tuple[str, tuple]:
        """Format PostgreSQL-specific FOR UPDATE expr.

        Handles PostgreSQL-specific lock strengths (FOR NO KEY UPDATE,
        FOR SHARE, FOR KEY SHARE) and options (NOWAIT, SKIP LOCKED).

        Args:
            expr: ForUpdateClause instance.

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
        from rhosocial.activerecord.backend.expression import LockStrength

        all_params = []

        strength = expr.strength

        # Check version support for lock strength
        if strength == LockStrength.UPDATE:
            if not self.supports_lock_strength_for_update():
                raise UnsupportedFeatureError(
                    self.name, "FOR UPDATE (unsupported lock strength)"
                )
        elif strength == LockStrength.NO_KEY_UPDATE:
            if not self.supports_lock_strength_no_key_update():
                raise UnsupportedFeatureError(
                    self.name, "FOR NO KEY UPDATE (requires PostgreSQL 9.0+)"
                )
        elif strength == LockStrength.SHARE:
            if not self.supports_lock_strength_share():
                raise UnsupportedFeatureError(
                    self.name, "FOR SHARE (requires PostgreSQL 9.0+)"
                )
        elif strength == LockStrength.KEY_SHARE:
            if not self.supports_lock_strength_key_share():
                raise UnsupportedFeatureError(
                    self.name, "FOR KEY SHARE (requires PostgreSQL 9.3+)"
                )
        else:
            raise UnsupportedFeatureError(
                self.name, f"{strength.value} (unsupported lock strength)"
            )

        # The strength's *word* travels in the artifact; the expr it writes
        # is a spelling, and ``phrase`` is where core keeps it (``FOR UPDATE``
        # &c.). Interpolating the word instead rendered a bare ``update``,
        # which the server rejects (#14 C).
        sql_parts = [strength.phrase]

        # Handle OF columns if specified
        if expr.of_columns:
            of_parts = []
            for col in expr.of_columns:
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
        if expr.nowait:
            if not self.supports_for_update_nowait():
                raise UnsupportedFeatureError(self.name, "NOWAIT")
            sql_parts.append("NOWAIT")
        elif expr.skip_locked:
            if not self.supports_for_update_skip_locked():
                raise UnsupportedFeatureError(
                    self.name, "SKIP LOCKED (requires PostgreSQL 9.5+)"
                )
            sql_parts.append("SKIP LOCKED")

        return " ".join(sql_parts), tuple(all_params)
