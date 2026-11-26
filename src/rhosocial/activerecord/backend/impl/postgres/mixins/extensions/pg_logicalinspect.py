# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_logicalinspect.py
"""Capability mixin for the pg_logicalinspect extension."""

class PostgresPgLogicalinspectMixin:
    """Logical decoding snapshot inspection."""

    def supports_pg_logicalinspect(self) -> bool:
        """Whether pg_logicalinspect capabilities are available."""
        return self.check_extension_feature("pg_logicalinspect", "snapshot")
