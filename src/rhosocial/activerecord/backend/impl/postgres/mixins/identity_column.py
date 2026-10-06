# src/rhosocial/activerecord/backend/impl/postgres/mixins/identity_column.py
"""PostgreSQL identity column feature support implementation.

PostgreSQL has the SQL-standard ``GENERATED {ALWAYS|BY DEFAULT} AS IDENTITY``
column clause since version 10. The boundary is measured, not assumed: on
PostgreSQL 9 the live server rejects the clause with ``syntax error at or near
"GENERATED"``, while 10 through 19 all accept it -- bare, ``ALWAYS``, and with
sequence options. The generation mode and every sequence option belong to the
same synopsis, so the six parameter probes are version-independent and answer
``True``; only the mechanism probe carries the version boundary.

PostgreSQL has no fallback to ``SERIAL``: ``SERIAL`` is a *type*, carried by
:class:`~...expression.types.PostgresSerialType` and its siblings, not an
identity clause. A user who wants SERIAL asks for that type explicitly; a
PostgreSQL 9 server that is asked for an identity column is refused rather
than handed a type it did not ask for.

These probes mirror
:class:`~...protocols.identity_column.PostgresIdentitySupport`. The protocol
and this mixin are held in step by ``TestProtocolMixinForwardCoverage`` and
``TestProtocolMixinReverseCoverage`` in ``test_postgres_protocol_conformance``,
which fail if a switch is declared in one and not the other.
"""

from rhosocial.activerecord.backend.dialect.mixins import IdentityColumnMixin


class PostgresIdentityColumnMixin(IdentityColumnMixin):
    """PostgreSQL identity column override implementation.

    The formatter itself stays core's: PostgreSQL uses the SQL-standard
    spelling, so ``format_identity_clause`` is inherited rather than
    overridden. Only the capability questions are answered here.
    """

    def supports_identity_column(self) -> bool:
        """Whether ``GENERATED ... AS IDENTITY`` can be used.

        ``True`` from PostgreSQL 10 on. PostgreSQL 9 has no identity columns:
        the live server rejects the clause with a syntax error, so the
        formatter must refuse rather than render it.
        """
        return self.version >= (10, 0, 0)

    def supports_identity_generation_always(self) -> bool:
        """``GENERATED ALWAYS AS IDENTITY`` is in the PostgreSQL synopsis."""
        return True

    def supports_identity_start(self) -> bool:
        """``START WITH`` is a sequence option of the identity clause."""
        return True

    def supports_identity_increment(self) -> bool:
        """``INCREMENT BY`` is a sequence option of the identity clause."""
        return True

    def supports_identity_minvalue(self) -> bool:
        """``MINVALUE`` is a sequence option of the identity clause."""
        return True

    def supports_identity_maxvalue(self) -> bool:
        """``MAXVALUE`` is a sequence option of the identity clause."""
        return True

    def supports_identity_cycle(self) -> bool:
        """``CYCLE`` / ``NO CYCLE`` is a sequence option of the identity clause."""
        return True
