# src/rhosocial/activerecord/backend/impl/postgres/protocols/auto_increment.py
"""PostgreSQL auto-increment marker support protocol.

Declared here, implemented by
:class:`~...mixins.auto_increment.PostgresAutoIncrementMixin`. PostgreSQL
answers the mechanism probe ``False`` -- ``AUTO_INCREMENT`` is MySQL / MariaDB
spelling -- so this protocol restates that one switch. The one-for-one mirror
is enforced by the forward/reverse coverage tests in
``test_postgres_protocol_conformance``.
"""

from typing import Protocol, runtime_checkable

from rhosocial.activerecord.backend.dialect.protocols import AutoIncrementColumnSupport


@runtime_checkable
class PostgresAutoIncrementSupport(AutoIncrementColumnSupport, Protocol):
    """PostgreSQL auto-increment marker support protocol.

    Derives from core's :class:`AutoIncrementColumnSupport` so the interface it
    restates is inherited rather than merely duplicated. The non-overlap guard
    in ``test_postgres_protocol_conformance`` reads that inheritance as the
    deliberate overlap it is.

    The body below restates the capability question so this protocol mirrors
    :class:`PostgresAutoIncrementMixin` method for method.
    """

    def supports_auto_increment_column(self) -> bool: ...
