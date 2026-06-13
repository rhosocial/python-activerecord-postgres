# src/rhosocial/activerecord/backend/impl/postgres/mixins/window.py
"""PostgreSQL window feature support implementation."""


class PostgresWindowMixin:
    """PostgreSQL window override implementation.

    All features are native, using version number for detection.
    """

    def supports_window_functions(self) -> bool:
        return True

    def supports_window_frame_clause(self) -> bool:
        return True
