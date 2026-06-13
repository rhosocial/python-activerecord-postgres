# src/rhosocial/activerecord/backend/impl/postgres/mixins/cte.py
"""PostgreSQL cte feature support implementation."""


class PostgresCTEMixin:
    """PostgreSQL cte override implementation.

    All features are native, using version number for detection.
    """

    def supports_basic_cte(self) -> bool:
        return True

    def supports_recursive_cte(self) -> bool:
        return True

    def supports_materialized_cte(self) -> bool:
        return self.version >= (12, 0, 0)
