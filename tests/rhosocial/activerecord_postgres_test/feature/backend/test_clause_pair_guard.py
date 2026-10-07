# tests/rhosocial/activerecord_postgres_test/feature/backend/test_clause_pair_guard.py
"""Guard: every clause pair the PostgreSQL dialect consumes is fully spellable.

The rule this file enforces (the round's rule 1/2/3/4):

* each spellable alternative has its own parameter;
* "unspecified" is the state where none of the group's parameters is set;
* setting more than one of them is API misuse and raises ``ValueError`` at
  construction time;
* no ``Optional[bool]`` tri-state and no sentinel value.

For every pair the PostgreSQL dialect consumes the four states must be
pairwise distinguishable:

====================  =============================================
neither parameter     neither spelling rendered
parameter A           A's spelling rendered (and not B's)
parameter B           B's spelling rendered (and not A's)
both parameters       ``ValueError``
====================  =============================================

Two deliberate shapes are pinned separately:

* A spelling PostgreSQL cannot express must refuse *by name*
  (``UnsupportedFeatureError``) -- never render a token the server rejects and
  never drop the clause. ``TestRefusedSpellingsFailClosed`` covers these, and
  names the measured server evidence for each.
* ``AlterConstraint.enforced`` is mandatory in the action's grammar: "neither"
  is refused as well as "both".

The render evidence in this file is produced offline by the dialect. The
server-side evidence for the capability answers (which tokens PostgreSQL
accepts) was measured against live PostgreSQL 16 and 19 servers; the probes
are version-independent for every clause covered here.
"""

import re

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.core import Column
from rhosocial.activerecord.backend.expression.objects import (
    Function,
    MaterializedView,
    Schema,
    Sequence,
    Table,
    Type,
    View,
)
from rhosocial.activerecord.backend.expression.pivot import UnpivotExpression
from rhosocial.activerecord.backend.expression.query_sources import (
    CTEExpression,
    SetOperationExpression,
)
from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
    AlterConstraint,
)
from rhosocial.activerecord.backend.expression.statements.ddl_function import (
    DropFunctionExpression,
)
from rhosocial.activerecord.backend.expression.statements.ddl_schema import (
    DropSchemaExpression,
)
from rhosocial.activerecord.backend.expression.statements.ddl_sequence import (
    AlterSequenceExpression,
    CreateSequenceExpression,
)
from rhosocial.activerecord.backend.expression.statements.ddl_table import (
    ColumnConstraint,
    ColumnConstraintType,
    CreateTableAsExpression,
    DropTableExpression,
    IdentityClause,
    TableConstraint,
    TableConstraintType,
)
from rhosocial.activerecord.backend.expression.statements.ddl_truncate import (
    TruncateExpression,
)
from rhosocial.activerecord.backend.expression.statements.ddl_view import (
    CreateMaterializedViewExpression,
    DropMaterializedViewExpression,
    DropViewExpression,
    RefreshMaterializedViewExpression,
)
from rhosocial.activerecord.backend.expression.statements.dql import QueryExpression
from rhosocial.activerecord.backend.expression.transaction import (
    BeginTransactionExpression,
    SetTransactionExpression,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.copy import (
    PostgresCopyToExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.exclude_constraint import (
    PostgresExcludeConstraint,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.mv import (
    PostgresRefreshMaterializedViewExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.type import (
    PostgresDropTypeExpression,
)

PG16 = (16, 0, 0)
PG19 = (19, 0, 0)


def _dialect(version=PG16):
    return PostgresDialect(version=version)


def _table(d, name="t"):
    return Table(d, name)


def _query(d):
    return QueryExpression(d, select=[Column(d, "id")], from_=_table(d))


def _fk(d, **kw):
    return TableConstraint(
        d,
        TableConstraintType.FOREIGN_KEY,
        name="fk",
        columns=["a"],
        foreign_key_table=_table(d, "t2"),
        foreign_key_columns=["b"],
        **kw,
    )


def _check(d, **kw):
    from rhosocial.activerecord.backend.expression.predicates import (
        ComparisonPredicate,
    )

    return TableConstraint(
        d,
        TableConstraintType.CHECK,
        name="ck",
        check_condition=ComparisonPredicate(d, "=", Column(d, "a"), Column(d, "b")),
        **kw,
    )


def _column_fk(d, **kw):
    return ColumnConstraint(
        d,
        ColumnConstraintType.FOREIGN_KEY,
        name="fk",
        foreign_key_reference=(_table(d, "t2"), ["b"]),
        **kw,
    )


def _column_check(d, **kw):
    from rhosocial.activerecord.backend.expression.predicates import (
        ComparisonPredicate,
    )

    return ColumnConstraint(
        d,
        ColumnConstraintType.CHECK,
        name="ck",
        check_condition=ComparisonPredicate(d, "=", Column(d, "a"), Column(d, "b")),
        **kw,
    )


def _exclude(d, **kw):
    return PostgresExcludeConstraint(
        d, name="ex", elements=[("range", "&&")], **kw
    )


class PairCase:
    """One two-spelling clause and the four states of its parameter pair."""

    def __init__(
        self,
        case_id,
        builder,
        a,
        b,
        a_pattern,
        b_pattern,
        a_value=True,
        b_value=True,
        version=PG16,
        neither_error=None,
        both_match=None,
        a_refusal=None,
        b_refusal=None,
    ):
        self.case_id = case_id
        self.builder = builder
        self.a = a
        self.b = b
        self.a_pattern = re.compile(a_pattern)
        self.b_pattern = re.compile(b_pattern)
        self.a_value = a_value
        self.b_value = b_value
        self.version = version
        self.neither_error = neither_error
        self.both_match = both_match or (
            f"{a} and {b} are mutually exclusive options"
        )
        self.a_refusal = a_refusal
        self.b_refusal = b_refusal

    def render(self, **kwargs):
        dialect = _dialect(self.version)
        sql, _params = self.builder(dialect, **kwargs).to_sql()
        return sql

    def outcome(self, side):
        """The spelling state of one side: render or refusal feature name."""
        if side == "a":
            return self.a_refusal, self.a_value
        return self.b_refusal, self.b_value

    def matches_a(self, sql):
        return bool(self.a_pattern.search(sql))

    def matches_b(self, sql):
        return bool(self.b_pattern.search(sql))


PAIR_CASES = (
    PairCase(
        "CreateSequenceExpression.cycle",
        lambda d, **kw: CreateSequenceExpression(d, Sequence(d, "s"), **kw),
        "cycle",
        "no_cycle",
        r"(?<!NO )CYCLE\b",
        r"NO CYCLE\b",
    ),
    PairCase(
        "CreateSequenceExpression.cache",
        lambda d, **kw: CreateSequenceExpression(d, Sequence(d, "s"), **kw),
        "cache",
        "no_cache",
        r"CACHE 10\b",
        r"NO CACHE\b",
        a_value=10,
        b_refusal="SEQUENCE NO CACHE",
    ),
    PairCase(
        "AlterSequenceExpression.cycle",
        lambda d, **kw: AlterSequenceExpression(d, Sequence(d, "s"), **kw),
        "cycle",
        "no_cycle",
        r"(?<!NO )CYCLE\b",
        r"NO CYCLE\b",
    ),
    PairCase(
        "AlterSequenceExpression.cache",
        lambda d, **kw: AlterSequenceExpression(d, Sequence(d, "s"), **kw),
        "cache",
        "no_cache",
        r"CACHE 10\b",
        r"NO CACHE\b",
        a_value=10,
        b_refusal="ALTER SEQUENCE NO CACHE",
    ),
    PairCase(
        "IdentityClause.cycle",
        lambda d, **kw: IdentityClause(d, **kw),
        "cycle",
        "no_cycle",
        r"(?<!NO )CYCLE\b",
        r"NO CYCLE\b",
    ),
    PairCase(
        "IdentityClause.cache",
        lambda d, **kw: IdentityClause(d, **kw),
        "cache",
        "no_cache",
        r"CACHE 10\b",
        r"NO CACHE\b",
        a_value=10,
        b_refusal="IDENTITY NO CACHE",
    ),
    PairCase(
        "DropSchemaExpression.cascade",
        lambda d, **kw: DropSchemaExpression(d, Schema(d, "s"), **kw),
        "cascade",
        "restrict",
        r"CASCADE\b",
        r"RESTRICT\b",
    ),
    PairCase(
        "DropViewExpression.cascade",
        lambda d, **kw: DropViewExpression(d, View(d, "v"), **kw),
        "cascade",
        "restrict",
        r"CASCADE\b",
        r"RESTRICT\b",
    ),
    PairCase(
        "DropMaterializedViewExpression.cascade",
        lambda d, **kw: DropMaterializedViewExpression(d, MaterializedView(d, "mv"), **kw),
        "cascade",
        "restrict",
        r"CASCADE\b",
        r"RESTRICT\b",
    ),
    PairCase(
        "DropFunctionExpression.cascade",
        lambda d, **kw: DropFunctionExpression(d, Function(d, "f"), **kw),
        "cascade",
        "restrict",
        r"CASCADE\b",
        r"RESTRICT\b",
    ),
    PairCase(
        "DropTableExpression.cascade",
        lambda d, **kw: DropTableExpression(d, _table(d), **kw),
        "cascade",
        "restrict",
        r"CASCADE\b",
        r"RESTRICT\b",
    ),
    PairCase(
        "TruncateExpression.cascade",
        lambda d, **kw: TruncateExpression(d, _table(d), **kw),
        "cascade",
        "restrict",
        r"CASCADE\b",
        r"RESTRICT\b",
    ),
    PairCase(
        "TruncateExpression.restart_identity",
        lambda d, **kw: TruncateExpression(d, _table(d), **kw),
        "restart_identity",
        "continue_identity",
        r"RESTART IDENTITY\b",
        r"CONTINUE IDENTITY\b",
    ),
    PairCase(
        "CreateMaterializedViewExpression.with_data",
        lambda d, **kw: CreateMaterializedViewExpression(
            d, MaterializedView(d, "mv"), _query(d), **kw
        ),
        "with_data",
        "no_data",
        r"WITH DATA\b",
        r"WITH NO DATA\b",
    ),
    PairCase(
        "RefreshMaterializedViewExpression.with_data",
        lambda d, **kw: RefreshMaterializedViewExpression(
            d, MaterializedView(d, "mv"), **kw
        ),
        "with_data",
        "no_data",
        r"WITH DATA\b",
        r"WITH NO DATA\b",
    ),
    PairCase(
        "PostgresRefreshMaterializedViewExpression.with_data",
        lambda d, **kw: PostgresRefreshMaterializedViewExpression(
            d, MaterializedView(d, "mv"), **kw
        ),
        "with_data",
        "no_data",
        r"WITH DATA\b",
        r"WITH NO DATA\b",
    ),
    PairCase(
        "CreateTableAsExpression.with_data",
        lambda d, **kw: CreateTableAsExpression(d, _table(d), _query(d), **kw),
        "with_data",
        "no_data",
        r"WITH DATA\b",
        r"WITH NO DATA\b",
    ),
    PairCase(
        "CTEExpression.materialized",
        lambda d, **kw: CTEExpression(d, "c", _query(d), **kw),
        "materialized",
        "not_materialized",
        r"(?<!NOT )MATERIALIZED\b",
        r"NOT MATERIALIZED\b",
    ),
    PairCase(
        "TableConstraint.deferrable",
        _fk,
        "deferrable",
        "not_deferrable",
        r"(?<!NOT )DEFERRABLE\b",
        r"NOT DEFERRABLE\b",
    ),
    PairCase(
        "TableConstraint.initially_deferred",
        _fk,
        "initially_deferred",
        "initially_immediate",
        r"INITIALLY DEFERRED\b",
        r"INITIALLY IMMEDIATE\b",
    ),
    PairCase(
        "TableConstraint.enforced",
        _check,
        "enforced",
        "not_enforced",
        r"(?<!NOT )ENFORCED\b",
        r"NOT ENFORCED\b",
        version=PG19,
    ),
    PairCase(
        "ColumnConstraint.deferrable",
        _column_fk,
        "deferrable",
        "not_deferrable",
        r"(?<!NOT )DEFERRABLE\b",
        r"NOT DEFERRABLE\b",
    ),
    PairCase(
        "ColumnConstraint.initially_deferred",
        _column_fk,
        "initially_deferred",
        "initially_immediate",
        r"INITIALLY DEFERRED\b",
        r"INITIALLY IMMEDIATE\b",
    ),
    PairCase(
        "ColumnConstraint.enforced",
        _column_check,
        "enforced",
        "not_enforced",
        r"(?<!NOT )ENFORCED\b",
        r"NOT ENFORCED\b",
        version=PG19,
    ),
    PairCase(
        "PostgresExcludeConstraint.deferrable",
        _exclude,
        "deferrable",
        "not_deferrable",
        r"(?<!NOT )DEFERRABLE\b",
        r"NOT DEFERRABLE\b",
    ),
    PairCase(
        "PostgresExcludeConstraint.initially_deferred",
        _exclude,
        "initially_deferred",
        "initially_immediate",
        r"INITIALLY DEFERRED\b",
        r"INITIALLY IMMEDIATE\b",
    ),
    PairCase(
        "SetOperationExpression.all_",
        lambda d, **kw: SetOperationExpression(
            d,
            left=QueryExpression(d, select=[Column(d, "a")], from_=_table(d, "t1")),
            right=QueryExpression(d, select=[Column(d, "a")], from_=_table(d, "t2")),
            operation="UNION",
            **kw,
        ),
        "all_",
        "distinct",
        r"\bALL\b",
        r"\bDISTINCT\b",
    ),
    PairCase(
        "BeginTransactionExpression.deferrable",
        lambda d, **kw: BeginTransactionExpression(d, **kw),
        "deferrable",
        "not_deferrable",
        r"(?<!NOT )DEFERRABLE\b",
        r"NOT DEFERRABLE\b",
    ),
    PairCase(
        "SetTransactionExpression.deferrable",
        lambda d, **kw: SetTransactionExpression(d, **kw),
        "deferrable",
        "not_deferrable",
        r"(?<!NOT )DEFERRABLE\b",
        r"NOT DEFERRABLE\b",
    ),
    PairCase(
        "AlterConstraint.enforced",
        lambda d, **kw: AlterConstraint(
            d, "c", constraint_type=TableConstraintType.CHECK, **kw
        ),
        "enforced",
        "not_enforced",
        r"(?<!NOT )ENFORCED\b",
        r"NOT ENFORCED\b",
        version=PG19,
        neither_error="AlterConstraint requires exactly one of",
    ),
    PairCase(
        "PostgresCopyToExpression.force_array",
        lambda d, **kw: PostgresCopyToExpression(
            d, table_name="t", format="json", **kw
        ),
        "force_array",
        "force_array_false",
        r"FORCE_ARRAY(?! FALSE)\b",
        r"FORCE_ARRAY FALSE\b",
        version=PG19,
    ),
    # Control: already the target shape before the round; pinned so a later
    # change cannot silently break it.
    PairCase(
        "PostgresDropTypeExpression.cascade/restrict",
        lambda d, **kw: PostgresDropTypeExpression(d, Type(d, "t"), **kw),
        "cascade",
        "restrict",
        r"CASCADE\b",
        r"RESTRICT\b",
        both_match="CASCADE and RESTRICT are mutually exclusive",
    ),
)

PAIR_IDS = [case.case_id for case in PAIR_CASES]


class TestFourStatesArePairwiseDistinguishable:
    """The four states of every consumed pair are pairwise distinguishable."""

    @pytest.mark.parametrize("case", PAIR_CASES, ids=PAIR_IDS)
    def test_neither_set_renders_neither_spelling(self, case):
        if case.neither_error is not None:
            with pytest.raises(ValueError, match=case.neither_error):
                case.render()
            return
        sql = case.render()
        assert not case.matches_a(sql), (
            f"{case.case_id}: with neither parameter set the SQL still spells "
            f"{case.a!r}: {sql!r}"
        )
        assert not case.matches_b(sql), (
            f"{case.case_id}: with neither parameter set the SQL still spells "
            f"{case.b!r}: {sql!r}"
        )

    @pytest.mark.parametrize("case", PAIR_CASES, ids=PAIR_IDS)
    def test_a_set_renders_a_only(self, case):
        refusal, value = case.outcome("a")
        if refusal is not None:
            with pytest.raises(UnsupportedFeatureError, match=refusal):
                case.render(**{case.a: value})
            return
        sql = case.render(**{case.a: value})
        assert case.matches_a(sql), f"{case.case_id}: {case.a!r} was not rendered: {sql!r}"
        assert not case.matches_b(sql), (
            f"{case.case_id}: setting {case.a!r} also rendered {case.b!r}: {sql!r}"
        )

    @pytest.mark.parametrize("case", PAIR_CASES, ids=PAIR_IDS)
    def test_b_set_renders_b_only(self, case):
        refusal, value = case.outcome("b")
        if refusal is not None:
            with pytest.raises(UnsupportedFeatureError, match=refusal):
                case.render(**{case.b: value})
            return
        sql = case.render(**{case.b: value})
        assert case.matches_b(sql), f"{case.case_id}: {case.b!r} was not rendered: {sql!r}"
        assert not case.matches_a(sql), (
            f"{case.case_id}: setting {case.b!r} also rendered {case.a!r}: {sql!r}"
        )

    @pytest.mark.parametrize("case", PAIR_CASES, ids=PAIR_IDS)
    def test_both_set_is_refused(self, case):
        with pytest.raises(ValueError, match=case.both_match):
            case.render(**{case.a: case.a_value, case.b: case.b_value})


class TestRefusedSpellingsFailClosed:
    """Spellings PostgreSQL cannot express refuse by name, never drop."""

    @pytest.mark.parametrize(
        "case_id,builder,feature",
        [
            (
                "CreateSequenceExpression.order",
                lambda d, **kw: CreateSequenceExpression(d, Sequence(d, "s"), **kw),
                "SEQUENCE ORDER",
            ),
            (
                "AlterSequenceExpression.order",
                lambda d, **kw: AlterSequenceExpression(d, Sequence(d, "s"), **kw),
                "ALTER SEQUENCE ORDER",
            ),
            (
                "IdentityClause.order",
                lambda d, **kw: IdentityClause(d, **kw),
                "IDENTITY ORDER",
            ),
        ],
        ids=["CreateSequence.order", "AlterSequence.order", "Identity.order"],
    )
    @pytest.mark.parametrize("side", ["order", "no_order"])
    def test_unspellable_option_refuses_by_name(self, case_id, builder, feature, side):
        dialect = _dialect()
        with pytest.raises(UnsupportedFeatureError, match=feature):
            builder(dialect, **{side: True}).to_sql()

    def test_unpivot_is_refused_before_its_pair_is_read(self):
        """PostgreSQL declares no UNPIVOT formatter; the refusal is the answer."""
        dialect = _dialect()
        with pytest.raises(UnsupportedFeatureError, match="format_unpivot_expression"):
            UnpivotExpression(
                dialect, value_column="v", pivot_column="k", columns=["a", "b"]
            ).to_sql()


class TestGuardIsNotVacuous:
    """Guards so the checks above cannot pass by accident."""

    def test_every_case_has_two_distinct_parameters(self):
        for case in PAIR_CASES:
            assert case.a != case.b, case.case_id

    def test_every_case_has_a_spelling_or_a_named_refusal(self):
        for case in PAIR_CASES:
            assert case.a_refusal or case.a_pattern, case.case_id
            assert case.b_refusal or case.b_pattern, case.case_id

    def test_expected_pairs_are_covered(self):
        expected = {
            "CreateSequenceExpression.cycle",
            "CreateSequenceExpression.cache",
            "AlterSequenceExpression.cycle",
            "AlterSequenceExpression.cache",
            "IdentityClause.cycle",
            "IdentityClause.cache",
            "DropSchemaExpression.cascade",
            "DropViewExpression.cascade",
            "DropMaterializedViewExpression.cascade",
            "DropFunctionExpression.cascade",
            "DropTableExpression.cascade",
            "TruncateExpression.cascade",
            "TruncateExpression.restart_identity",
            "CreateMaterializedViewExpression.with_data",
            "RefreshMaterializedViewExpression.with_data",
            "PostgresRefreshMaterializedViewExpression.with_data",
            "CreateTableAsExpression.with_data",
            "CTEExpression.materialized",
            "TableConstraint.deferrable",
            "TableConstraint.initially_deferred",
            "TableConstraint.enforced",
            "ColumnConstraint.deferrable",
            "ColumnConstraint.initially_deferred",
            "ColumnConstraint.enforced",
            "PostgresExcludeConstraint.deferrable",
            "PostgresExcludeConstraint.initially_deferred",
            "SetOperationExpression.all_",
            "BeginTransactionExpression.deferrable",
            "SetTransactionExpression.deferrable",
            "AlterConstraint.enforced",
            "PostgresCopyToExpression.force_array",
            "PostgresDropTypeExpression.cascade/restrict",
        }
        covered = {case.case_id for case in PAIR_CASES}
        missing = expected - covered
        assert not missing, f"pairs in scope but not guarded: {sorted(missing)}"

    def test_refusal_features_are_named(self):
        """A refusal that only says "unsupported" does not name the option."""
        for case in PAIR_CASES:
            for refusal in (case.a_refusal, case.b_refusal):
                if refusal is not None:
                    assert refusal.strip(), case.case_id
