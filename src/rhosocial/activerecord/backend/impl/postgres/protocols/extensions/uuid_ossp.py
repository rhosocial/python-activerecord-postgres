# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/uuid_ossp.py
"""uuid-ossp extension protocol definition.

This module defines the protocol for uuid-ossp UUID generation
functionality in PostgreSQL.

For SQL expression generation, use the function factories in
``functions/uuid.py`` instead of the removed format_* methods.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresUuidOssSupport(Protocol):
    """uuid-ossp UUID generation extension protocol.

    Feature Source: Extension support (requires uuid-ossp extension)

    uuid-ossp provides UUID generation functions:
    - Generate UUIDs using various algorithms
    - Version 1 (MAC address + time)
    - Version 4 (random)

    Extension Information:
    - Extension name: uuid-ossp
    - Install command: CREATE EXTENSION uuid-ossp;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/uuid-ossp.html

    It used to declare ``supports_uuid_generation``, which collided with the
    core :class:`UUIDSupport` on the same name, and the overlap check rejected
    the pair. The overlap was a real ambiguity, not bookkeeping: this protocol
    asked whether the extension is *installed*, while the core asked whether
    the server can *generate* a UUID. Two questions, two names.

    ``supports_uuid_generation`` now answers both at once and lives only in the
    core protocol; :class:`PostgresUUIDMixin` folds the 13.0 built-in and this
    extension into that single answer. What remains here is the narrower
    question, under a name that cannot be confused with it.
    """

    def supports_uuid_ossp_extension(self) -> bool:
        """Whether the uuid-ossp extension is installed on this database.

        Distinct from generating a UUID: 13.0+ servers generate one with no
        extension at all, so a True here says nothing about whether
        ``supports_uuid_generation()`` is True.
        """
        ...
