# src/rhosocial/activerecord/backend/impl/postgres/mixins/lateral_join.py
"""PostgreSQL lateral join feature support implementation."""


class PostgresLateralJoinMixin:
    """PostgreSQL lateral join override implementation.

    All features are native, using version number for detection.
    """

    def supports_lateral_join(self) -> bool:
        return self.version >= (9, 3, 0)
