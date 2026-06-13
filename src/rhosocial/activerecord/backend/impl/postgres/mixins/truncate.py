# src/rhosocial/activerecord/backend/impl/postgres/mixins/truncate.py
"""PostgreSQL truncate feature support implementation."""


class PostgresTruncateMixin:
    """PostgreSQL truncate override implementation.

    All features are native, using version number for detection.
    """

    def supports_truncate_restart_identity(self) -> bool:
        return self.version >= (8, 4, 0)

    def supports_truncate_cascade(self) -> bool:
        return True
