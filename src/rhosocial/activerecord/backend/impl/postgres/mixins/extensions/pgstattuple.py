# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pgstattuple.py
"""Capability mixin for the pgstattuple extension."""

class PostgresPgstattupleMixin:
    """Tuple-level statistics."""

    def supports_pgstattuple(self) -> bool:
        """Whether pgstattuple capabilities are available."""
        return self.check_extension_feature("pgstattuple", "tuple_stats")
