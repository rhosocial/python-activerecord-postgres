# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/amcheck.py
"""Capability mixin for the amcheck extension."""

class PostgresAmcheckMixin:
    """Index and heap consistency checking."""

    def supports_amcheck(self) -> bool:
        """Whether amcheck capabilities are available."""
        return self.check_extension_feature("amcheck", "integrity")
