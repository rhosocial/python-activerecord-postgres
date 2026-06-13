# src/rhosocial/activerecord/backend/impl/postgres/mixins/set_operation.py
"""PostgreSQL set operation feature support implementation."""


class PostgresSetOperationMixin:
    """PostgreSQL set operation override implementation.

    All features are native, using version number for detection.
    """

    def supports_union(self) -> bool:
        return True

    def supports_union_all(self) -> bool:
        return True

    def supports_intersect(self) -> bool:
        return True

    def supports_except(self) -> bool:
        return True

    def supports_set_operation_order_by(self) -> bool:
        return True

    def supports_set_operation_limit_offset(self) -> bool:
        return True

    def supports_set_operation_for_update(self) -> bool:
        return True
