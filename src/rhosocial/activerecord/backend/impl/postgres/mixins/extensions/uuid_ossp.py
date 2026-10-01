# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/uuid_ossp.py
"""PostgreSQL uuid-ossp extension mixin.

The extension is queried through ``check_extension_feature`` rather than
through a method of its own. This class used to define
``supports_uuid_generation``, which meant two probes shared one name and two
questions — "is uuid-ossp installed" here and "can this server generate a
UUID" in ``PostgresUUIDMixin`` — and the MRO decided which answer a caller
got. The single merged answer now lives in ``PostgresUUIDMixin``.
"""


class PostgresUuidOssMixin:
    """uuid-ossp UUID generation functionality implementation.

    Intentionally empty of public methods: see the module docstring. The
    extension is still reachable through ``check_extension_feature("uuid_ossp",
    ...)``, and ``PostgresUUIDMixin`` consults it when deciding whether
    ``supports_uuid_generation()`` is true.
    """
