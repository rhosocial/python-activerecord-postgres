# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/uuid_ossp.py
"""PostgreSQL uuid-ossp extension mixin.

Exposes whether the extension is installed. It used to define
``supports_uuid_generation`` as well, which meant two probes shared one name
and two questions — "is uuid-ossp installed" here and "can this server
generate a UUID" in ``PostgresUUIDMixin`` — and the MRO decided which answer a
caller got. The merged answer now lives in ``PostgresUUIDMixin``; the narrower
question is answerable here, under its own name.
"""


class PostgresUuidOssMixin:
    """uuid-ossp UUID generation functionality implementation."""

    def supports_uuid_ossp_extension(self) -> bool:
        """Whether the uuid-ossp extension is installed on this database."""
        return self.check_extension_feature("uuid_ossp", "generation")
