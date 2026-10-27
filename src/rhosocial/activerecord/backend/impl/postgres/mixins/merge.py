# src/rhosocial/activerecord/backend/impl/postgres/mixins/merge.py
"""PostgreSQL merge feature support implementation."""


class PostgresMergeMixin:
    """PostgreSQL merge override implementation.

    All features are native, using version number for detection.
    """

    def supports_merge_statement(self) -> bool:
        return self.version_at_least((15, 0, 0))
