# src/rhosocial/activerecord/backend/impl/postgres/protocols/sequence.py
"""PostgreSQL sequence feature support protocol.

Declared here, implemented by
:class:`~...mixins.sequence.PostgresSequenceMixin`. PostgreSQL is a
full-support sequence dialect, so this restates every capability switch the
core sequence protocols declare and answers them True -- except
``supports_sequence_order``, because the ``CREATE SEQUENCE`` synopsis has no
``ORDER`` / ``NO ORDER`` clause. The one-for-one mirror is enforced by the
forward/reverse coverage tests in ``test_postgres_protocol_conformance``.
"""

from typing import Protocol, runtime_checkable, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.protocols import (
    AlterSequenceSupport,
    CreateSequenceSupport,
    DropSequenceSupport,
)

if TYPE_CHECKING:
    from ...expression.statements.ddl_sequence import (
        AlterSequenceExpression,
        CreateSequenceExpression,
    )


@runtime_checkable
class PostgresSequenceSupport(
    CreateSequenceSupport, DropSequenceSupport, AlterSequenceSupport, Protocol
):
    """PostgreSQL sequence feature support protocol.

    Derives from the three core per-statement sequence protocols -- core split
    the former umbrella into CREATE / DROP / ALTER -- so the interface it
    restates is inherited rather than merely duplicated. The non-overlap guard
    in ``test_postgres_protocol_conformance`` reads that inheritance as the
    deliberate overlap it is; a standalone protocol declaring the same switches
    would look like an accidental collision instead.

    The bodies below restate each capability question so this protocol mirrors
    :class:`PostgresSequenceMixin` method for method.
    """

    def supports_sequence(self) -> bool: ...

    def supports_create_sequence(self) -> bool: ...

    def supports_drop_sequence(self) -> bool: ...

    def supports_alter_sequence(self) -> bool: ...

    def supports_alter_sequence_start(self) -> bool: ...

    def supports_sequence_if_not_exists(self) -> bool: ...

    def supports_sequence_if_exists(self) -> bool: ...

    def supports_sequence_start(self) -> bool: ...

    def supports_sequence_increment(self) -> bool: ...

    def supports_sequence_minvalue(self) -> bool: ...

    def supports_sequence_maxvalue(self) -> bool: ...

    def supports_sequence_cycle(self) -> bool: ...

    def supports_sequence_cache(self) -> bool: ...

    def supports_sequence_order(self) -> bool: ...

    def supports_sequence_owned_by(self) -> bool: ...

    def format_create_sequence_statement(
        self, expr: "CreateSequenceExpression"
    ) -> Tuple[str, tuple]: ...

    def format_alter_sequence_statement(
        self, expr: "AlterSequenceExpression"
    ) -> Tuple[str, tuple]: ...
