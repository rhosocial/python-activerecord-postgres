# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/unaccent.py
"""PostgreSQL unaccent extension mixin.

``unaccent`` is a contrib module: it is available on every supported server,
but must be enabled per database with ``CREATE EXTENSION unaccent``.
"""


class PostgresUnaccentMixin:
    """unaccent text normalization capability."""

    def supports_unaccent(self) -> bool:
        """Whether the ``unaccent()`` function is available."""
        return self.check_extension_feature("unaccent", "function")
