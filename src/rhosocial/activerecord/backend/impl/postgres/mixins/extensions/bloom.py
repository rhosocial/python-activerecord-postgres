# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/bloom.py
"""bloom capability probe. Index DDL comes from ``functions.bloom.bloom_index``."""


class PostgresBloomMixin:
    """bloom filter index functionality implementation."""

    def supports_bloom_index(self) -> bool:
        """Check if bloom index is supported."""
        return self.check_extension_feature("bloom", "index")

    def render_bloom_access_method(self) -> str:
        """Render the bloom access-method setting (a string utility)."""
        return "SET default_index_access_method = 'bloom'"
