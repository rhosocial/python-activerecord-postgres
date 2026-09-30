# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/uuid_ossp.py
"""
PostgreSQL uuid-ossp UUID generation functionality mixin.

This module provides functionality to check uuid-ossp extension features.

For SQL expression generation, use the function factories in
``functions/uuid.py`` instead of the removed format_* methods.
"""


class PostgresUuidOssMixin:
    """uuid-ossp UUID generation functionality implementation."""

    def supports_uuid_generation(self) -> bool:
        """Whether UUID generation is available on this server.

        The dialect answers this from :class:`PostgresUUIDMixin`, which folds
        both routes together: the built-in ``gen_random_uuid()`` on 13.0+ and
        ``uuid_generate_v4()`` when uuid-ossp is installed. This method exists
        so the class still satisfies :class:`PostgresUuidOssSupport`; on a
        dialect it is shadowed by that mixin, and it must not become a second,
        different answer.
        """
        from ..uuid import PostgresUUIDMixin

        return PostgresUUIDMixin.supports_uuid_generation(self)
