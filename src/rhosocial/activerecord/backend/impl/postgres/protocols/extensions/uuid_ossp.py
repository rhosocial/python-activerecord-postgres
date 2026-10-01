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

    This protocol deliberately declares no methods. It used to declare
    ``supports_uuid_generation``, which collided with the core
    :class:`UUIDSupport` on the same name — the overlap check rejected the two
    protocols, and the overlap was a real ambiguity rather than a bookkeeping
    problem. The question "is uuid-ossp installed" is a property of the
    database, not of the dialect, so the answer belongs to
    ``check_extension_feature``; the question "can this server generate a
    UUID" belongs to ``UUIDSupport.supports_uuid_generation``, which answers
    it once by folding in both the 13.0 built-in and the extension route.
    """
