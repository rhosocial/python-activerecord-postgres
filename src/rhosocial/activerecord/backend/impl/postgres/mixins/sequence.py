# src/rhosocial/activerecord/backend/impl/postgres/mixins/sequence.py
"""PostgreSQL sequence feature support implementation.

PostgreSQL is a full-support sequence dialect. Its ``CREATE SEQUENCE`` synopsis
takes ``IF NOT EXISTS``, ``INCREMENT BY``, ``MINVALUE``, ``MAXVALUE``,
``CYCLE``, ``START WITH``, ``CACHE`` and ``OWNED BY``; the one option core's
``CreateSequenceExpression`` carries that PostgreSQL has no clause for is
``ORDER``, which is Oracle/SQL Server syntax and is absent from the synopsis.
The probes below restate that, so the core formatters -- which consult
:meth:`supports_sequence` first and each option probe before its clause -- can
render everything the server accepts and refuse only ``ORDER``.

These probes mirror
:class:`~...protocols.sequence.PostgresSequenceSupport`. The protocol and this
mixin are held in step by ``TestProtocolMixinForwardCoverage`` and
``TestProtocolMixinReverseCoverage`` in ``test_postgres_protocol_conformance``,
which fail if a switch is declared in one and not the other.
"""


class PostgresSequenceMixin:
    """PostgreSQL sequence override implementation.

    Every form below has been accepted since PostgreSQL 9.6, this backend's
    baseline, so the answers are version-independent and the ``version`` the
    dialect carries does not enter into them.
    """

    def supports_sequence(self) -> bool:
        return True

    def supports_create_sequence(self) -> bool:
        return True

    def supports_drop_sequence(self) -> bool:
        return True

    def supports_alter_sequence(self) -> bool:
        return True

    def supports_alter_sequence_start(self) -> bool:
        """Whether ``ALTER SEQUENCE ... START WITH`` is supported.

        This is a different clause from the ``CREATE SEQUENCE ... START WITH``
        one :meth:`supports_sequence_start` describes, so ``True`` there does
        not imply ``True`` here and the two are not interchangeable. PostgreSQL
        accepts both: ``ALTER SEQUENCE ... START WITH n`` records the start
        value future ``RESTART`` calls use, while ``RESTART WITH n`` sets the
        current value.

        The default is ``False``, which is the safe side: Oracle, SQL Server,
        Firebird and Snowflake all reject ``START`` on ``ALTER SEQUENCE``, and
        a probe answering ``True`` by default would let them emit SQL their
        server rejects. A dialect that does accept the clause must therefore
        say so explicitly, as this one does.
        """
        return True

    def supports_sequence_if_not_exists(self) -> bool:
        return True

    def supports_sequence_if_exists(self) -> bool:
        return True

    def supports_sequence_start(self) -> bool:
        return True

    def supports_sequence_increment(self) -> bool:
        return True

    def supports_sequence_minvalue(self) -> bool:
        return True

    def supports_sequence_maxvalue(self) -> bool:
        return True

    def supports_sequence_cycle(self) -> bool:
        return True

    def supports_sequence_cache(self) -> bool:
        return True

    def supports_sequence_order(self) -> bool:
        # ORDER / NO ORDER is Oracle/SQL Server syntax. The PostgreSQL
        # CREATE SEQUENCE synopsis has no such clause, so this is the one
        # sequence option the dialect must refuse.
        return False

    def supports_sequence_owned_by(self) -> bool:
        return True
