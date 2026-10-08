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

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.statements.ddl_sequence import (
        AlterSequenceExpression,
        CreateSequenceExpression,
    )


class PostgresSequenceMixin:
    """PostgreSQL sequence override implementation.

    Every form below has been accepted since PostgreSQL 9.6, this backend's
    baseline, so the answers are version-independent and the ``version`` the
    dialect carries does not enter into them.

    One spelling the CACHE option pair carries has no PostgreSQL form:
    ``NO CACHE``. The ``CREATE SEQUENCE`` / ``ALTER SEQUENCE`` synopsis reads
    ``[ CACHE cache ]`` with a positive minimum, so the uncached form is
    ``CACHE 1``; ``NO CACHE`` is a syntax error (measured on PostgreSQL 16,
    ``syntax error at or near "CACHE"``). The formatters below therefore refuse
    ``no_cache=True`` by name rather than render a token the server rejects --
    the same fail-closed rule the core formatters apply to options whose probe
    is False, applied here to the one spelling of an option whose probe is True.
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

    # ------------------------------------------------------------------
    # Formatter overrides: the one spelling PostgreSQL cannot express
    # ------------------------------------------------------------------

    def _refuse_no_cache(self, feature: str) -> None:
        """Refuse ``NO CACHE`` by name; PostgreSQL spells it ``CACHE 1``."""
        from rhosocial.activerecord.backend.dialect.exceptions import (
            UnsupportedFeatureError,
        )

        raise UnsupportedFeatureError(
            self.name,
            feature,
            f"{self.name} has no NO CACHE spelling; cache=1 is the uncached "
            f"form. The CACHE option itself is supported.",
        )

    def format_create_sequence_statement(self, expr: "CreateSequenceExpression") -> Tuple[str, tuple]:
        """Render ``CREATE SEQUENCE``, refusing the unspellable negative.

        Delegates to the core formatter for everything else so the option
        gating and ordering stay in one place. ``no_cache`` is the one spelling
        PostgreSQL's synopsis lacks; it is refused by name rather than rendered.
        """
        if expr.no_cache:
            self._refuse_no_cache("SEQUENCE NO CACHE")
        return super().format_create_sequence_statement(expr)

    def format_alter_sequence_statement(self, expr: "AlterSequenceExpression") -> Tuple[str, tuple]:
        """Render ``ALTER SEQUENCE``, refusing the unspellable negative.

        See :meth:`format_create_sequence_statement`.
        """
        if expr.no_cache:
            self._refuse_no_cache("ALTER SEQUENCE NO CACHE")
        return super().format_alter_sequence_statement(expr)
