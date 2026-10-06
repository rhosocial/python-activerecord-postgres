# src/rhosocial/activerecord/backend/impl/postgres/protocols/identity_column.py
"""PostgreSQL identity column feature support protocol.

Declared here, implemented by
:class:`~...mixins.identity_column.PostgresIdentityColumnMixin`. PostgreSQL
uses the SQL-standard ``GENERATED {ALWAYS|BY DEFAULT} AS IDENTITY`` spelling,
so the formatter is core's; this protocol restates the mechanism probe and the
six option probes the mixin answers. The one-for-one mirror is enforced by the
forward/reverse coverage tests in ``test_postgres_protocol_conformance``.
"""

from typing import Protocol, runtime_checkable

from rhosocial.activerecord.backend.dialect.protocols import IdentityColumnSupport


@runtime_checkable
class PostgresIdentitySupport(IdentityColumnSupport, Protocol):
    """PostgreSQL identity column feature support protocol.

    Derives from core's :class:`IdentityColumnSupport` so the interface it
    restates is inherited rather than merely duplicated. The non-overlap guard
    in ``test_postgres_protocol_conformance`` reads that inheritance as the
    deliberate overlap it is; a standalone protocol declaring the same switches
    would look like an accidental collision instead.

    The bodies below restate each capability question so this protocol mirrors
    :class:`PostgresIdentityColumnMixin` method for method.
    """

    def supports_identity_column(self) -> bool: ...

    def supports_identity_generation_always(self) -> bool: ...

    def supports_identity_start(self) -> bool: ...

    def supports_identity_increment(self) -> bool: ...

    def supports_identity_minvalue(self) -> bool: ...

    def supports_identity_maxvalue(self) -> bool: ...

    def supports_identity_cycle(self) -> bool: ...
