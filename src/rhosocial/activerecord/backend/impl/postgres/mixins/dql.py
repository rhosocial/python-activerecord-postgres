# src/rhosocial/activerecord/backend/impl/postgres/mixins/dql.py
"""PostgreSQL DQL feature support mixin."""


class PostgresDQLMixin:
    """PostgreSQL DQL feature support."""

    def supports_for_update(self) -> bool:
        """Whether FOR UPDATE clause is supported in SELECT statements.

        PostgreSQL supports FOR UPDATE since early versions. The clause locks
        selected rows preventing other transactions from modifying them.
        PostgreSQL also supports FOR UPDATE OF, FOR UPDATE NOWAIT, and
        FOR UPDATE SKIP LOCKED (since 9.5).
        """
        return True
