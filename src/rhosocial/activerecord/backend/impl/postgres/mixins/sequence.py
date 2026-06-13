# src/rhosocial/activerecord/backend/impl/postgres/mixins/sequence.py
"""PostgreSQL sequence feature support implementation."""


class PostgresSequenceMixin:
    """PostgreSQL sequence override implementation.

    All features are native, using version number for detection.
    """

    def supports_create_sequence(self) -> bool:
        return True

    def supports_drop_sequence(self) -> bool:
        return True
