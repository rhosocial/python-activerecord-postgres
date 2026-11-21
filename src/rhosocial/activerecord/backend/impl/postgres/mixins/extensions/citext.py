# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/citext.py
"""citext capability probe.

Column definitions come from ``functions.citext.citext_column``.
"""



class PostgresCitextMixin:
    """citext case-insensitive text functionality implementation."""

    def supports_citext_type(self) -> bool:
        """Check if citext type is supported."""
        return self.check_extension_feature("citext", "type")
