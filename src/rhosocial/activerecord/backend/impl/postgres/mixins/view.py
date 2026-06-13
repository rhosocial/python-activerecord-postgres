# src/rhosocial/activerecord/backend/impl/postgres/mixins/view.py
"""PostgreSQL view feature support implementation."""


class PostgresViewMixin:
    """PostgreSQL view feature support implementation."""

    def supports_or_replace_view(self) -> bool:
        return True

    def supports_temporary_view(self) -> bool:
        return True

    def supports_if_exists_view(self) -> bool:
        return True

    def supports_view_check_option(self) -> bool:
        return True

    def supports_cascade_view(self) -> bool:
        return True

    def supports_materialized_view(self) -> bool:
        return True

    def supports_refresh_materialized_view(self) -> bool:
        return True

    def supports_materialized_view_tablespace(self) -> bool:
        return True

    def supports_materialized_view_storage_options(self) -> bool:
        return True
