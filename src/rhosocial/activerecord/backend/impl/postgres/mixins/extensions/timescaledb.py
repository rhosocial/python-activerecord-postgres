# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/timescaledb.py
"""Capability mixin for the TimescaleDB extension."""


class PostgreTimescaleDBMixin:
    """TimescaleDB hypertable capability."""

    def supports_timescaledb(self) -> bool:
        """Whether TimescaleDB hypertable features are available."""
        return self.check_extension_feature("timescaledb", "hypertable")
