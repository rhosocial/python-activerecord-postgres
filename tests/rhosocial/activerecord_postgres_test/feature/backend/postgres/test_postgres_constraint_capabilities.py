# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_postgres_constraint_capabilities.py
"""PostgreSQL constraint capability switches, pinned to live measurements.

Every value asserted here was measured against the instances
``tests/config/postgres_scenarios.yaml`` names, all on ``192.168.1.3``,
through psycopg 3.3.6, each probe inside a transaction that was rolled back:

* ``postgres_19beta4`` -- port 16690, ``server_version_num`` 190000,
  ``PostgreSQL 19beta4 (Debian 19~beta4-1.pgdg13+1) on
  aarch64-unknown-linux-gnu``
* ``postgres_18`` -- port 16689, ``server_version_num`` 180006
* ``postgres_17`` -- port 16688, ``server_version_num`` 170011

The statements and the server's own SQLSTATEs are recorded in the docstrings of
``rhosocial.activerecord.backend.impl.postgres.mixins.ddl.constraint``; this
module pins the values and checks that they are load-bearing.

Three guards beyond the values themselves:

* **declared here, not inherited.** Each switch must be declared by a class in
  *this* package, so a change to a core default cannot silently move
  PostgreSQL's contract.  Several of the switches below are ``True`` in core's
  shared ``ConstraintMixin`` too; the point of declaring them again is that a
  moved default becomes a loud test failure rather than a silent change of
  what this backend renders.
* **the answer decides the outcome.** For every switch core consults, the
  render/refuse behaviour must follow the value.
* **the version gates are measured too.** The ``ENFORCED`` boundaries were
  checked against all three instances, because the 19beta4 instance alone
  cannot tell 18 from 19.

The per-kind ``DEFERRABLE`` split and the ``ENFORCED`` per-kind refusals are
asserted in ``test_ddl_alter_config.py`` (``TestPostgresConstraintDeferral`` and
``TestPostgresConstraintEnforcement``); this module covers the switches that
have no argument and the declaration-locality guard for all of them.
"""

import re

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import Column, ComparisonPredicate
from rhosocial.activerecord.backend.expression.statements import (
    AddTableConstraintAction,
    DropTableConstraintAction,
    TableCheckConstraint,
    TableConstraint,
    TableForeignKeyConstraint,
    TablePrimaryKeyConstraint,
    TableUniqueConstraint,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect

PG_17 = (17, 0, 0)
PG_18 = (18, 0, 0)
PG_19 = (19, 0, 0)

#: Every capability this package declares must come from this package.
BACKEND_PACKAGE = "rhosocial.activerecord.backend.impl.postgres."

#: (switch name, measured value) pairs. The value is hard-coded on purpose:
#: reading it back from the dialect would make the test unable to see a
#: moved default.
MEASURED_SWITCHES = (
    # -- ALTER TABLE constraint lifecycle (SQL:92). All accepted on 19beta4.
    ("supports_add_constraint", True),
    ("supports_drop_constraint", True),
    ("supports_rename_constraint", True),
    ("supports_drop_constraint_if_exists", True),
    # -- Referential actions and match modes (SQL:1999).
    ("supports_fk_on_delete", True),
    ("supports_fk_on_update", True),
    ("supports_fk_match", True),
    # -- Deferral: supported, but NOT for every kind. The grammar figure
    #    draws [DEFERRABLE] as a tail property of every branch; the server
    #    refuses it for CHECK (0A000 at table level, 42601 misplaced at
    #    column level) and for NOT NULL (42601 misplaced).
    ("supports_deferrable_constraint", True),
    ("supports_deferrable_primary_key_constraint", True),
    ("supports_deferrable_unique_constraint", True),
    ("supports_deferrable_foreign_key_constraint", True),
    ("supports_deferrable_exclude_constraint", True),
    ("supports_deferrable_check_constraint", False),
    ("supports_deferrable_not_null_constraint", False),
    # -- Lifecycle verbs the server does not have.
    #    "ENABLE/DISABLE CONSTRAINT" is not PostgreSQL syntax at all:
    #    42601 syntax error at or near "CONSTRAINT" on every instance.
    ("supports_enable_disable_constraint", False),
    ("supports_validate_constraint", True),
    ("supports_constraint_novalidate", True),
    ("supports_exclude_constraint", True),
)


#: The two enforcement probes, whose value is a version gate rather than a
#: fixed answer -- so they are pinned with their boundary, not with a single
#: value, and they are the switches that converged onto the ``(self)``
#: signature this round.
VERSION_GATED_SWITCHES = (
    # 170011: 42601, the clause is not in the grammar for any kind.
    ("supports_constraint_enforced", (PG_17, False), (PG_18, True), (PG_19, True)),
    ("supports_alter_constraint_enforced", (PG_17, False), (PG_18, True), (PG_19, True)),
)

#: Every switch this backend relies on must be declared in this package,
#: whichever of the two tables it is in.
ALL_SWITCHES = tuple(s for s, _ in MEASURED_SWITCHES) + tuple(
    s for s, _, _, _ in VERSION_GATED_SWITCHES
)


def _dialect(version=PG_19):
    return PostgresDialect(version)


def _declaring_class(switch_name):
    """The first class in the dialect's MRO that declares ``switch_name``."""
    for cls in PostgresDialect.__mro__:
        if switch_name in cls.__dict__:
            return cls
    raise AssertionError(f"no class in the MRO declares {switch_name}()")


class TestSwitchResolutionIsOwnedByThisBackend:
    """The resolved answer is this backend's, not a core default reached past it.

    ``_declaring_class`` walks the MRO from the dialect outwards, so the first
    class that has the name in ``__dict__`` is the one that answers.  That is
    the whole point of declaring locally: a later mixin in the same MRO cannot
    quietly take a switch back.  These guards fail the moment that happens,
    rather than leaving it to be noticed in a rendering difference.
    """

    @pytest.mark.parametrize("switch_name", ALL_SWITCHES)
    def test_resolved_switch_is_owned_by_this_backend(self, switch_name):
        declaring = _declaring_class(switch_name)
        assert declaring.__module__.startswith(BACKEND_PACKAGE), (
            f"{switch_name} resolves to {declaring.__module__}.{declaring.__qualname__}; "
            f"a core default answers first, so this backend's measured value is "
            f"never consulted."
        )

    @pytest.mark.parametrize("switch_name", ALL_SWITCHES)
    def test_the_declaring_class_is_a_postgres_mixin(self, switch_name):
        """The class that answers lives in this backend, whatever mixin owns it.

        Not every switch belongs to ``PostgresConstraintMixin`` -- ``supports_
        drop_constraint_if_exists`` is declared by the ALTER COLUMN modifier,
        which reaches the MRO first.  What matters is that the answer is a
        PostgreSQL one and that the class it comes from is findable, so a
        reader tracing the contract knows where to look.
        """
        declaring = _declaring_class(switch_name)
        assert declaring.__module__.startswith(BACKEND_PACKAGE), (
            f"{switch_name} is declared by {declaring.__module__}; the class that "
            f"answers must be part of this backend."
        )

    @pytest.mark.parametrize(
        "switch_name,_v17,_v18,_v19", VERSION_GATED_SWITCHES,
        ids=[s for s, _, _, _ in VERSION_GATED_SWITCHES],
    )
    def test_probes_take_no_argument(self, switch_name, _v17, _v18, _v19):
        """A probe answers one question and takes no kind.

        The signature is the contract: core calls ``supports_*()`` with no
        arguments, so a parameterised probe is either never consulted or
        consulted with a guess.  ``(self)`` and nothing else.
        """
        import inspect

        bound = getattr(_dialect(), switch_name)
        assert not [
            name
            for name in inspect.signature(bound).parameters.values()
            if name.default is not inspect.Parameter.empty
            and name.kind
            not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
        ], f"{switch_name} still takes a parameter; a probe names no kind."

    @pytest.mark.parametrize(
        "switch_name,at17,at18,at19", VERSION_GATED_SWITCHES,
        ids=[s for s, _, _, _ in VERSION_GATED_SWITCHES],
    )
    def test_version_gate_matches_the_measurement(self, switch_name, at17, at18, at19):
        for version, expected in (at17, at18, at19):
            assert _dialect(version).__getattribute__(switch_name)() is expected, (
                f"{switch_name} moved at {version}; the boundary is measured on all "
                f"three instances (170011 / 180006 / 190000), not read off a table."
            )


#: A five-digit SQLSTATE is what a server answers with; the instances in
#: ``postgres_scenarios.yaml`` are what the answers came from.
_SQLSTATE = r"\b[0-9A-Z]{5}\b"
_MEASURED_WITNESS = re.compile(
    rf"({_SQLSTATE}|PostgreSQL \d|accepted|is not PostgreSQL syntax)", re.IGNORECASE
)


class TestEverySwitchCarriesItsMeasurement:
    """A switch whose docstring says only ``True`` is a guess with a name.

    The rule this enforces is the one the whole draft was written for: a value
    is trustworthy when the statement that produced it is next to it.  So each
    declared switch's docstring has to quote something falsifiable -- a
    SQLSTATE, a server version, or the server's own "accepted" verdict -- and a
    switch that starts documenting without one fails here.
    """

    @pytest.mark.parametrize("switch_name", ALL_SWITCHES)
    def test_docstring_quotes_the_measurement(self, switch_name):
        declaring = _declaring_class(switch_name)
        doc = (declaring.__dict__[switch_name].__doc__ or "").strip()
        assert doc, f"{switch_name} has no docstring at all."
        assert _MEASURED_WITNESS.search(doc), (
            f"{switch_name}()'s docstring quotes no measurement. Either paste the "
            f"statement the value came from with the server's own answer, or say "
            f"why there is no statement to quote."
        )


class TestMeasuredConstraintSwitchValues:
    @pytest.mark.parametrize(
        "switch_name,expected", MEASURED_SWITCHES, ids=[s for s, _ in MEASURED_SWITCHES]
    )
    def test_matches_measurement(self, switch_name, expected):
        dialect = _dialect()
        assert getattr(dialect, switch_name)() is expected, (
            f"{switch_name}() no longer matches the PostgreSQL 19beta4 measurement; "
            f"see the docstring in mixins/ddl/constraint.py for the statement that "
            f"decided it."
        )

    @pytest.mark.parametrize(
        "switch_name,_expected", MEASURED_SWITCHES, ids=[s for s, _ in MEASURED_SWITCHES]
    )
    def test_declared_by_this_backend(self, switch_name, _expected):
        """A moved core default must not move PostgreSQL's contract silently."""
        declaring = _declaring_class(switch_name)
        assert declaring.__module__.startswith(BACKEND_PACKAGE), (
            f"{switch_name} is inherited from {declaring.__module__}; declare it "
            f"in this backend's mixins so the measured value is explicit."
        )

    def test_version_does_not_move_the_unconditional_values(self):
        """Most of these are statement-level facts, not version gates.

        Measured on 17 / 18 / 19beta4; asserting they hold there is the claim
        this backend has actually made, so the check is that no version branch
        exists for them.
        """
        for version in (PG_17, PG_18, PG_19):
            dialect = PostgresDialect(version)
            for switch_name, expected in MEASURED_SWITCHES:
                if switch_name in {"supports_constraint_novalidate"}:
                    # NOT VALID is the one value whose 17 boundary was not
                    # probed; keep it out of this claim rather than assert
                    # something the probe did not establish.
                    continue
                assert (
                    getattr(dialect, switch_name)() is expected
                ), f"{switch_name} moved with version {version}"


class TestEnforcementVersionBoundaries:
    """``ENFORCED`` boundaries, measured on all three instances.

    170011 -> ``42601: syntax error at or near "ENFORCED"`` for every kind.
    180006 -> ADD works for CHECK and FOREIGN KEY; ALTER works for FOREIGN KEY
              only (``42809`` for CHECK).
    190000 -> both statements work for both kinds.

    Both switches take no kind: a probe answers "is the capability family here
    at all", and the per-kind answer is the dialect's narrowing. What is
    measured here is therefore the *verb gate*, and the per-kind behaviour is
    asserted where it happens -- ``format_table_constraint`` /
    ``_require_constraint_enforced`` for the ADD side,
    ``format_alter_constraint_action`` / ``_require_enforceable_constraint`` for
    the ALTER side.
    """

    def test_grammar_gate_is_18_on_both_statements(self):
        for version, expected in ((PG_17, False), (PG_18, True), (PG_19, True)):
            dialect = PostgresDialect(version)
            assert dialect.supports_constraint_enforced() is expected
            assert dialect.supports_alter_constraint_enforced() is expected

    def test_add_side_narrows_to_check_and_foreign_key(self):
        """``0A000: ... constraints cannot be marked NOT ENFORCED`` on 18 and 19."""
        for version in (PG_18, PG_19):
            dialect = PostgresDialect(version)
            assert dialect._require_constraint_enforced(TableCheckConstraint) is None
            assert dialect._require_constraint_enforced(TableForeignKeyConstraint) is None
            for branch in (TablePrimaryKeyConstraint, TableUniqueConstraint):
                with pytest.raises(UnsupportedFeatureError):
                    dialect._require_constraint_enforced(branch)

    def test_alter_side_splits_foreign_key_at_18_and_check_at_19(self):
        # 170011: 42601 for both kinds -- the clause is not in the grammar.
        pg17 = PostgresDialect(PG_17)
        with pytest.raises(UnsupportedFeatureError):
            pg17._require_enforceable_constraint(TableForeignKeyConstraint)
        with pytest.raises(UnsupportedFeatureError):
            pg17._require_enforceable_constraint(TableCheckConstraint)

        # 180006: foreign key accepted, check refused with 42809.
        pg18 = PostgresDialect(PG_18)
        assert pg18._require_enforceable_constraint(TableForeignKeyConstraint) is None
        with pytest.raises(UnsupportedFeatureError):
            pg18._require_enforceable_constraint(TableCheckConstraint)

        # 190000: both accepted.
        pg19 = PostgresDialect(PG_19)
        assert pg19._require_enforceable_constraint(TableForeignKeyConstraint) is None
        assert pg19._require_enforceable_constraint(TableCheckConstraint) is None

    def test_a_kind_that_is_not_a_branch_class_is_a_type_error(self):
        """The kind is a class now; a spelling is a type question."""
        dialect = PostgresDialect(PG_19)
        for helper in (dialect._require_constraint_enforced, dialect._require_enforceable_constraint):
            with pytest.raises(TypeError, match="constraint branch class"):
                helper("CHECK")


class TestAddAndDropAndRenameRender:
    """The three lifecycle switches decide these renders."""

    def test_add_table_constraint_renders(self):
        d = _dialect()
        predicate = ComparisonPredicate(d, "=", Column(d, "a"), Column(d, "b"))
        action = AddTableConstraintAction(
            d, TableConstraint(d, branch=TableCheckConstraint(d, predicate), name='ck')
        )
        sql, params = action.to_sql()
        assert sql == 'ADD CONSTRAINT "ck" CHECK ("a" = "b")'
        assert params == ()

    def test_drop_constraint_renders(self):
        d = _dialect()
        sql, params = DropTableConstraintAction(d, "ck").to_sql()
        assert sql == 'DROP CONSTRAINT "ck"'
        assert params == ()

    def test_drop_constraint_if_exists_renders(self):
        """``IF EXISTS`` is the idempotent path: the plain form is 42704."""
        d = _dialect()
        sql, params = DropTableConstraintAction(d, "ck", if_exists=True).to_sql()
        assert sql == 'DROP CONSTRAINT IF EXISTS "ck"'
        assert params == ()
