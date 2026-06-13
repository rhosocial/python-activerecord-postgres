# src/rhosocial/activerecord/backend/impl/postgres/mixins/join.py
"""PostgreSQL join feature support implementation."""


class PostgresJoinMixin:
    """PostgreSQL join override implementation.

    All features are native, using version number for detection.
    """

    def supports_right_join(self) -> bool:
        return True

    def supports_full_join(self) -> bool:
        return True

    def supports_wildcard(self) -> bool:
        return True
