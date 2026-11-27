# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/functions/test_hint_plan_unaccent.py
"""SQL construction tests for pg_hint_plan and unaccent factories."""

import inspect

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import Column, QueryStatement
from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.objects import (
    Index,
    MaterializedView,
    Table,
    View,
)
from rhosocial.activerecord.backend.expression.sources import NamedRelationRef
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.statements.ddl_table import (
    CreateTableAsStatement,
)
from rhosocial.activerecord.backend.expression.statements.ddl_view import (
    CreateMaterializedViewStatement,
    CreateViewStatement,
    RefreshMaterializedViewStatement,
)
from rhosocial.activerecord.backend.expression.statements.dml import (
    DeleteStatement,
    InsertStatement,
    SelectSource,
    UpdateStatement,
)
from rhosocial.activerecord.backend.expression.statements.explain import ExplainStatement
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.hint import (
    CorrectionKind,
    HINT_FIELDS,
    HintGroup,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.mv import (
    PostgresCreateMaterializedViewExpression,
    PostgresRefreshMaterializedViewStatement,
)
from rhosocial.activerecord.backend.impl.postgres.expression.hinted_statements import (
    PostgresCreateTableAsStatement,
    PostgresDeleteStatement,
    PostgresExplainStatement,
    PostgresInsertStatement,
    PostgresQueryStatement,
    PostgresUpdateStatement,
)
from rhosocial.activerecord.backend.impl.postgres.functions.pg_hint_plan import (
    hint_bitmap_scan,
    hint_hash_join,
    hint_index_scan,
    hint_leading,
    hint_merge_join,
    hint_nest_loop,
    hint_no_seq_scan,
    hint_parallel,
    hint_rows,
    hint_seq_scan,
    hint_set,
    load_pg_hint_plan,
)
from rhosocial.activerecord.backend.impl.postgres.functions.unaccent import unaccent
from rhosocial.activerecord.backend.impl.postgres.protocols.base import PostgresExtensionInfo


@pytest.fixture
def dialect():
    """A dialect that has pg_hint_plan available.

    A hinted statement renders only on a dialect whose ``supports_pg_hint_plan()``
    answers true. The extension is simulated here because these are construction
    tests: no server is contacted.

    The version is pinned past every gate the statements below consult:
    materialized views need 9.3+ and ``IF NOT EXISTS`` / ``CONCURRENTLY`` 9.4+.
    """
    dialect = PostgresDialect(version=(16, 0, 0))
    dialect._extensions = {
        "pg_hint_plan": PostgresExtensionInfo(
            name="pg_hint_plan", installed=True, available=True, version="1.8.1"
        )
    }
    return dialect


def _hinted_query(dialect, hint):
    """The smallest derived statement that renders one hint phrase."""
    return PostgresQueryStatement(
        dialect,
        select=[Column(dialect, "id")],
        from_=Table(dialect, "t"),
        hints=(hint,),
    )


def _plain_query(dialect):
    """The same query as core's generic statement: a hint adds a parameter only."""
    return QueryStatement(dialect, select=[Column(dialect, "id")], from_=Table(dialect, "t"))


def _phrase(dialect, hint):
    """The hint phrase a statement renders, without the surrounding SQL."""
    sql, _ = _hinted_query(dialect, hint).to_sql()
    return sql.split(" */ ")[0][len("/*+ ") :]


class TestHintBuilders:
    @pytest.mark.parametrize(
        ("factory", "expected"),
        [
            (lambda d: hint_seq_scan(d, "t"), "SeqScan(t)"),
            (lambda d: hint_index_scan(d, "t", "idx"), "IndexScan(t idx)"),
            (lambda d: hint_bitmap_scan(d, "t", "idx1", "idx2"), "BitmapScan(t idx1 idx2)"),
            (lambda d: hint_no_seq_scan(d, "t"), "NoSeqScan(t)"),
            (lambda d: hint_hash_join(d, "a", "b"), "HashJoin(a b)"),
            (lambda d: hint_merge_join(d, "a", "b", "c"), "MergeJoin(a b c)"),
            (lambda d: hint_nest_loop(d, "a", "b"), "NestLoop(a b)"),
            (lambda d: hint_leading(d, "a", "b"), "Leading(a b)"),
            (lambda d: hint_rows(d, CorrectionKind.ABSOLUTE, 100, "a", "b"), "Rows(a b #100)"),
            (lambda d: hint_parallel(d, "t", 2), "Parallel(t 2 soft)"),
            (lambda d: hint_parallel(d, "t", 0, soft=False), "Parallel(t 0 hard)"),
            (lambda d: hint_set(d, "random_page_cost", 1.1), "Set(random_page_cost 1.1)"),
        ],
    )
    def test_phrases(self, dialect, factory, expected):
        assert _phrase(dialect, factory(dialect)) == expected

    def test_nested_leading(self, dialect):
        hint = hint_leading(dialect, HintGroup("a", "b"), "c")
        assert hint["sequence"] == (("a", "b"), "c")
        assert _phrase(dialect, hint) == "Leading((a b) c)"

    @pytest.mark.parametrize("table", ["t;DROP TABLE t", "a b", "", "1bad"])
    def test_untrusted_identifiers_rejected(self, dialect, table):
        # The builder collects; the renderer refuses the identifier.
        hint = hint_seq_scan(dialect, table)
        with pytest.raises(TypeError):
            _hinted_query(dialect, hint).to_sql()

    @pytest.mark.parametrize("amount", ["100", None, True])
    def test_bad_rows_amount_rejected(self, dialect, amount):
        hint = hint_rows(dialect, CorrectionKind.ABSOLUTE, amount, "a")
        with pytest.raises(TypeError):
            _hinted_query(dialect, hint).to_sql()

    @pytest.mark.parametrize("guc,value", [("bad-name", "1"), ("valid", "1;2")])
    def test_bad_guc_rejected(self, dialect, guc, value):
        hint = hint_set(dialect, guc, value)
        with pytest.raises(TypeError):
            _hinted_query(dialect, hint).to_sql()


class TestHintTargets:
    """A hint target is a relation reference, resolved to one wire spelling."""

    def test_alias_wins(self, dialect):
        ref = NamedRelationRef(dialect, Table(dialect, "t1"), alias="a")
        payload = hint_index_scan(dialect, ref)
        assert payload["tables"] == ('"a"',), "an alias is the only spelling that matches"
        assert _phrase(dialect, payload) == 'IndexScan("a")'

    def test_unaliased_reference_yields_the_relation_name(self, dialect):
        ref = NamedRelationRef(dialect, Table(dialect, "t1"))
        payload = hint_seq_scan(dialect, ref)
        assert payload["tables"] == ('"t1"',)
        assert _phrase(dialect, payload) == 'SeqScan("t1")'

    def test_alias_without_quoting(self, dialect):
        ref = NamedRelationRef(dialect, Table(dialect, "t1"), alias="a", alias_need_quote=False)
        assert hint_seq_scan(dialect, ref)["tables"] == ("a",)

    def test_schema_is_never_emitted(self, dialect):
        table = Table(dialect, "t1", schema_name="s2")
        payload = hint_seq_scan(dialect, table)
        assert payload["tables"] == ('"t1"',), "a bare name still matches s2.t1"
        assert "." not in payload["tables"][0], "a schema-qualified hint does not parse"

    def test_a_quoted_name_survives_to_the_wire(self, dialect):
        table = Table(dialect, "Odd Name")
        payload = hint_index_scan(dialect, table)
        assert payload["tables"] == ('"Odd Name"',), "quoting is exact, not cosmetic"
        assert _phrase(dialect, payload) == 'IndexScan("Odd Name")'

    def test_a_mixed_case_name_is_not_lowercased(self, dialect):
        table = Table(dialect, "Mixed")
        assert hint_seq_scan(dialect, table)["tables"] == ('"Mixed"',)

    def test_a_view_is_refused(self, dialect):
        with pytest.raises(TypeError) as caught:
            hint_seq_scan(dialect, View(dialect, "v1"))
        assert "view" in str(caught.value)
        assert "expands" in str(caught.value)

    def test_a_non_relation_object_is_refused(self, dialect):
        with pytest.raises(TypeError) as caught:
            hint_index_scan(dialect, Index(dialect, "ix"))
        assert "not a relation" in str(caught.value)

    def test_an_unknown_target_type_is_refused(self, dialect):
        with pytest.raises(TypeError) as caught:
            hint_seq_scan(dialect, 42)
        assert "int" in str(caught.value)

    def test_a_hint_group_takes_references(self, dialect):
        group = HintGroup(
            Table(dialect, "a", name_need_quote=False),
            NamedRelationRef(
                dialect,
                Table(dialect, "b", name_need_quote=False),
                alias="b2",
                alias_need_quote=False,
            ),
        )
        hint = hint_leading(dialect, group, Table(dialect, "c", name_need_quote=False))
        assert hint["sequence"] == (("a", "b2"), "c")
        assert _phrase(dialect, hint) == "Leading((a b2) c)"

    def test_a_decoded_payload_renders_as_is(self, dialect):
        payload = hint_seq_scan(dialect, Table(dialect, "Odd Name"))
        # An artifact decodes JSON arrays as lists; the resolved strings are
        # already on the wire and must not be resolved a second time.
        decoded = dict(payload, tables=list(payload["tables"]))
        assert _phrase(dialect, decoded) == 'SeqScan("Odd Name")'

    def test_a_pseudo_name_is_a_string_escape_hatch(self, dialect):
        assert hint_seq_scan(dialect, "*VALUES*")["tables"] == ("*VALUES*",)


class TestHintPayload:
    def test_a_hint_is_a_plain_dict_payload(self):
        hint = hint_seq_scan(PostgresDialect(), "t")
        assert isinstance(hint, dict), "the payload must be a dict on the wire"
        assert not isinstance(hint, BaseExpression), "a hint has no standalone SQL"
        assert not hasattr(hint, "format_method"), "a hint must not be dispatchable"

    def test_every_key_is_present(self, dialect):
        payload = hint_seq_scan(dialect, "t")
        assert set(payload) == set(HINT_FIELDS)
        assert payload["kind"] == "SeqScan"
        assert payload["tables"] == ("t",)
        assert payload["indexes"] == ()
        assert payload["sequence"] == ()
        assert payload["correction_kind"] is None
        assert payload["workers"] is None
        assert payload["guc"] is None

    def test_rows_fills_the_correction_fields(self, dialect):
        payload = hint_rows(dialect, CorrectionKind.ABSOLUTE, 100, "a", "b")
        assert payload["correction_kind"] == "#"
        assert payload["correction_amount"] == 100

    def test_parallel_fills_the_worker_fields(self, dialect):
        payload = hint_parallel(dialect, "t", 4, soft=False)
        assert payload["workers"] == 4
        assert payload["soft"] is False

    def test_set_fills_the_guc_fields(self, dialect):
        payload = hint_set(dialect, "enable_hashjoin", "off")
        assert payload["guc"] == "enable_hashjoin"
        assert payload["guc_value"] == "off"


#: The six statements this backend derives from core. The derived class adds
#: only ``hints``: every other parameter name, kind, default and annotation
#: must be the base's, or ``get_params()`` would name a parameter the base
#: constructor cannot accept.
DERIVED_FROM_CORE = [
    (PostgresQueryStatement, QueryStatement),
    (PostgresInsertStatement, InsertStatement),
    (PostgresUpdateStatement, UpdateStatement),
    (PostgresDeleteStatement, DeleteStatement),
    (PostgresExplainStatement, ExplainStatement),
    (PostgresCreateTableAsStatement, CreateTableAsStatement),
]

#: The two materialized-view statements this backend already derived before
#: hints. Their signatures were their own, so the pre-change parameter list is
#: recorded here; the only delta this change may add is the trailing
#: keyword-only ``hints``.
PG_ONLY_PRE_CHANGE_SIGNATURES = [
    (
        PostgresCreateMaterializedViewExpression,
        (
            "dialect",
            "view",
            "query",
            "column_aliases",
            "tablespace",
            "with_data",
            "no_data",
            "storage_options",
            "if_not_exists",
            "allow_unlisted_storage_parameters",
        ),
    ),
    (
        PostgresRefreshMaterializedViewStatement,
        ("dialect", "view", "concurrently", "with_data", "no_data"),
    ),
]


class TestDerivedStatements:
    """A hint rides on a PostgreSQL statement; core's generic one stays untouched."""

    @pytest.mark.parametrize(("derived", "base"), DERIVED_FROM_CORE)
    def test_derived_adds_only_a_keyword_only_hints_parameter(self, derived, base):
        base_params = list(inspect.signature(base.__init__).parameters.values())
        derived_params = list(inspect.signature(derived.__init__).parameters.values())
        assert len(derived_params) == len(base_params) + 1, (
            f"{derived.__name__} must add exactly one parameter; "
            f"it declares {[p.name for p in derived_params]}"
        )
        for base_param, derived_param in zip(base_params, derived_params):
            assert derived_param.name == base_param.name
            assert derived_param.kind == base_param.kind
            assert derived_param.default == base_param.default
            assert derived_param.annotation == base_param.annotation
        extra = derived_params[-1]
        assert extra.name == "hints"
        assert extra.kind is inspect.Parameter.KEYWORD_ONLY
        assert extra.default == ()
        assert extra.annotation is not inspect.Parameter.empty

    @pytest.mark.parametrize(("derived", "pre_change"), PG_ONLY_PRE_CHANGE_SIGNATURES)
    def test_pg_only_statements_gained_only_hints(self, derived, pre_change):
        params = list(inspect.signature(derived.__init__).parameters.values())
        assert tuple(p.name for p in params[1:-1]) == pre_change, (
            f"{derived.__name__} changed a pre-existing parameter, not just "
            f"the hint support: {[p.name for p in params]}"
        )
        assert all(p.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD for p in params[1:-1])
        extra = params[-1]
        assert extra.name == "hints"
        assert extra.kind is inspect.Parameter.KEYWORD_ONLY
        assert extra.default == ()
        assert extra.annotation is not inspect.Parameter.empty

    @pytest.mark.parametrize(("derived", "base"), DERIVED_FROM_CORE)
    def test_the_derived_class_inherits_the_base_format_method(self, derived, base):
        """No new expression/formatter pair: the kind alone is not dispatched."""
        assert derived.format_method == base.format_method, (
            "a derived statement must inherit the base format_method, not " "declare a second one"
        )

    def test_the_pg_only_statements_keep_their_own_formatters(self):
        """The two existing PG statements keep the dispatch they already had."""
        assert (
            PostgresCreateMaterializedViewExpression.format_method
            == CreateMaterializedViewStatement.format_method
        )
        assert (
            PostgresRefreshMaterializedViewStatement.format_method.fget(None)
            == "format_refresh_materialized_view_pg_statement"
        )

    def test_query_prefix_and_params(self, dialect):
        expr = _hinted_query(dialect, hint_seq_scan(dialect, "t"))
        assert expr.to_sql() == ('/*+ SeqScan(t) */ SELECT "id" FROM "t"', ())

    def test_multiple_hints_share_one_comment(self, dialect):
        hints = [hint_leading(dialect, "a", "b"), hint_hash_join(dialect, "a", "b")]
        expr = PostgresQueryStatement(
            dialect, [Column(dialect, "id")], Table(dialect, "t"), hints=tuple(hints)
        )
        assert expr.to_sql()[0] == '/*+ Leading(a b) HashJoin(a b) */ SELECT "id" FROM "t"'

    def test_a_dict_hint_is_accepted(self, dialect):
        payload = hint_seq_scan(dialect, "t")
        expr = PostgresQueryStatement(
            dialect, [Column(dialect, "id")], Table(dialect, "t"), hints=(payload,)
        )
        assert expr.to_sql()[0] == '/*+ SeqScan(t) */ SELECT "id" FROM "t"'

    def test_insert_update_delete(self, dialect):
        table = Table(dialect, "t")
        inserts = PostgresInsertStatement(
            dialect,
            table,
            ValuesSource(dialect, [[Literal(dialect, 1)]]),
            ["a"],
            hints=(hint_seq_scan(dialect, "t"),),
        )
        updates = PostgresUpdateStatement(
            dialect,
            table,
            {"a": Literal(dialect, 1)},
            hints=(hint_parallel(dialect, "t", 4),),
        )
        deletes = PostgresDeleteStatement(
            dialect, table, hints=(hint_index_scan(dialect, "t", "i"),)
        )
        assert inserts.to_sql() == (
            '/*+ SeqScan(t) */ INSERT INTO "t" ("a") VALUES (%s)',
            (1,),
        )
        assert updates.to_sql() == (
            '/*+ Parallel(t 4 soft) */ UPDATE "t" SET "a" = %s',
            (1,),
        )
        assert deletes.to_sql() == ('/*+ IndexScan(t i) */ DELETE FROM "t"', ())

    def test_explain_prefix(self, dialect):
        query = _plain_query(dialect)
        expr = PostgresExplainStatement(dialect, query, hints=(hint_seq_scan(dialect, "t"),))
        assert expr.to_sql() == ('/*+ SeqScan(t) */ EXPLAIN SELECT "id" FROM "t"', ())

    def test_empty_hints_render_the_plain_statement(self, dialect):
        """An empty payload list is "no hints", not a refused statement."""
        expr = PostgresQueryStatement(
            dialect, [Column(dialect, "id")], Table(dialect, "t"), hints=()
        )
        assert expr.to_sql() == ('SELECT "id" FROM "t"', ())

    def test_non_hint_rejected(self, dialect):
        expr = PostgresQueryStatement(
            dialect,
            [Column(dialect, "id")],
            Table(dialect, "t"),
            hints=("SeqScan(t)",),
        )
        with pytest.raises(TypeError):
            expr.to_sql()

    def test_load_statement(self, dialect):
        assert load_pg_hint_plan(dialect).to_sql() == ("LOAD 'pg_hint_plan'", ())


#: A generic statement of each kind, rendered with no hint parameter at all.
#: These are the bytes this backend produced before hints existed; a hint adds
#: a comment prefix and must change nothing else.
PLAIN_STATEMENTS = [
    (
        lambda d: QueryStatement(d, [Column(d, "id")], Table(d, "t")),
        ('SELECT "id" FROM "t"', ()),
    ),
    (
        lambda d: InsertStatement(d, Table(d, "t"), ValuesSource(d, [[Literal(d, 1)]]), ["a"]),
        ('INSERT INTO "t" ("a") VALUES (%s)', (1,)),
    ),
    (
        lambda d: UpdateStatement(d, Table(d, "t"), {"a": Literal(d, 1)}),
        ('UPDATE "t" SET "a" = %s', (1,)),
    ),
    (
        lambda d: DeleteStatement(d, Table(d, "t")),
        ('DELETE FROM "t"', ()),
    ),
    (
        lambda d: ExplainStatement(d, QueryStatement(d, [Column(d, "id")], Table(d, "t"))),
        ('EXPLAIN SELECT "id" FROM "t"', ()),
    ),
    (
        lambda d: CreateTableAsStatement(
            d, Table(d, "ct"), QueryStatement(d, [Column(d, "id")], Table(d, "t"))
        ),
        ('CREATE TABLE "ct" AS SELECT "id" FROM "t"', ()),
    ),
    (
        lambda d: CreateMaterializedViewStatement(
            d, MaterializedView(d, "mv"), QueryStatement(d, [Column(d, "id")], Table(d, "t"))
        ),
        ('CREATE MATERIALIZED VIEW "mv" AS SELECT "id" FROM "t"', ()),
    ),
    (
        lambda d: RefreshMaterializedViewStatement(d, MaterializedView(d, "mv")),
        ('REFRESH MATERIALIZED VIEW "mv"', ()),
    ),
]


class TestGenericStatements:
    """The formatter override handles both the generic expression and the derived one."""

    @pytest.mark.parametrize(("builder", "expected"), PLAIN_STATEMENTS)
    def test_plain_base_statement_renders_unchanged(self, dialect, builder, expected):
        assert builder(dialect).to_sql() == expected

    def test_explain_of_a_plain_statement_has_no_prefix(self, dialect):
        query = _plain_query(dialect)
        assert ExplainStatement(dialect, query).to_sql() == (
            'EXPLAIN SELECT "id" FROM "t"',
            (),
        )


class TestHintedDdlStatements:
    """The head hint also rides on statements PostgreSQL plans when it runs.

    Measured against pg_hint_plan 1.8.1 on PG 18: ``CREATE TABLE ... AS``,
    ``CREATE MATERIALIZED VIEW ... AS`` and ``REFRESH MATERIALIZED VIEW`` each
    report ``used hint`` for a head comment and choose a different plan. Without
    hints the statements below are the exact bytes this backend produced before
    hints existed, so the prefix is the only change.
    """

    def test_create_table_as_prefix(self, dialect):
        plain = CreateTableAsStatement(dialect, Table(dialect, "ct"), _plain_query(dialect))
        assert plain.to_sql() == ('CREATE TABLE "ct" AS SELECT "id" FROM "t"', ())

        hinted = PostgresCreateTableAsStatement(
            dialect,
            Table(dialect, "ct"),
            _plain_query(dialect),
            hints=(hint_seq_scan(dialect, "t"),),
        )
        assert hinted.to_sql() == (
            '/*+ SeqScan(t) */ CREATE TABLE "ct" AS SELECT "id" FROM "t"',
            (),
        )

    def test_create_materialized_view_prefix(self, dialect):
        plain = CreateMaterializedViewStatement(
            dialect, MaterializedView(dialect, "mv"), _plain_query(dialect)
        )
        assert plain.to_sql() == (
            'CREATE MATERIALIZED VIEW "mv" AS SELECT "id" FROM "t"',
            (),
        )

        hinted = PostgresCreateMaterializedViewExpression(
            dialect,
            MaterializedView(dialect, "mv"),
            _plain_query(dialect),
            hints=(hint_seq_scan(dialect, "t"),),
        )
        assert hinted.to_sql() == (
            '/*+ SeqScan(t) */ CREATE MATERIALIZED VIEW "mv" AS SELECT "id" FROM "t"',
            (),
        )

    def test_refresh_materialized_view_prefix(self, dialect):
        plain = RefreshMaterializedViewStatement(dialect, MaterializedView(dialect, "mv"))
        assert plain.to_sql() == ('REFRESH MATERIALIZED VIEW "mv"', ())

        hinted = PostgresRefreshMaterializedViewStatement(
            dialect,
            MaterializedView(dialect, "mv"),
            hints=(hint_seq_scan(dialect, "mv"),),
        )
        assert hinted.to_sql() == (
            '/*+ SeqScan(mv) */ REFRESH MATERIALIZED VIEW "mv"',
            (),
        )


class TestNestedHintRefusal:
    """A hint belongs to the statement whose SQL is planned, not to a nested one."""

    def test_explain_refuses_a_hinted_query(self, dialect):
        with pytest.raises(TypeError) as caught:
            ExplainStatement(dialect, _hinted_query(dialect, hint_seq_scan(dialect, "t"))).to_sql()
        assert "outermost statement" in str(caught.value)

    def test_hinted_explain_refuses_a_hinted_query(self, dialect):
        expr = PostgresExplainStatement(
            dialect,
            _hinted_query(dialect, hint_seq_scan(dialect, "t")),
            hints=(hint_seq_scan(dialect, "t"),),
        )
        with pytest.raises(TypeError) as caught:
            expr.to_sql()
        assert "outermost statement" in str(caught.value)

    def test_create_table_as_refuses_a_hinted_query(self, dialect):
        expr = CreateTableAsStatement(
            dialect, Table(dialect, "ct"), _hinted_query(dialect, hint_seq_scan(dialect, "t"))
        )
        with pytest.raises(TypeError) as caught:
            expr.to_sql()
        assert "outermost statement" in str(caught.value)

    def test_create_materialized_view_refuses_a_hinted_query(self, dialect):
        expr = CreateMaterializedViewStatement(
            dialect,
            MaterializedView(dialect, "mv"),
            _hinted_query(dialect, hint_seq_scan(dialect, "t")),
        )
        with pytest.raises(TypeError) as caught:
            expr.to_sql()
        assert "outermost statement" in str(caught.value)

    def test_insert_refuses_a_hinted_select_source(self, dialect):
        query = _hinted_query(dialect, hint_seq_scan(dialect, "t"))
        expr = InsertStatement(dialect, Table(dialect, "t"), SelectSource(dialect, query), ["a"])
        with pytest.raises(TypeError) as caught:
            expr.to_sql()
        assert "outermost statement" in str(caught.value)

    def test_create_view_refuses_a_hinted_query(self, dialect):
        """A view is not planned at creation time, so a hinted body is inert."""
        expr = CreateViewStatement(
            dialect, View(dialect, "v"), _hinted_query(dialect, hint_seq_scan(dialect, "t"))
        )
        with pytest.raises(TypeError) as caught:
            expr.to_sql()
        assert "outermost statement" in str(caught.value)


#: A derived statement carrying a hint, one per kind, on a dialect without the
#: extension. The extension probe, not the statement type, answers whether the
#: comment can be spelled.
HINTED_STATEMENTS = [
    lambda d: PostgresQueryStatement(
        d, [Column(d, "id")], Table(d, "t"), hints=(hint_seq_scan(d, "t"),)
    ),
    lambda d: PostgresInsertStatement(
        d, Table(d, "t"), ValuesSource(d, [[Literal(d, 1)]]), ["a"], hints=(hint_seq_scan(d, "t"),)
    ),
    lambda d: PostgresUpdateStatement(
        d, Table(d, "t"), {"a": Literal(d, 1)}, hints=(hint_seq_scan(d, "t"),)
    ),
    lambda d: PostgresDeleteStatement(d, Table(d, "t"), hints=(hint_seq_scan(d, "t"),)),
    lambda d: PostgresExplainStatement(
        d, QueryStatement(d, [Column(d, "id")], Table(d, "t")), hints=(hint_seq_scan(d, "t"),)
    ),
    lambda d: PostgresCreateTableAsStatement(
        d,
        Table(d, "ct"),
        QueryStatement(d, [Column(d, "id")], Table(d, "t")),
        hints=(hint_seq_scan(d, "t"),),
    ),
    lambda d: PostgresCreateMaterializedViewExpression(
        d,
        MaterializedView(d, "mv"),
        QueryStatement(d, [Column(d, "id")], Table(d, "t")),
        hints=(hint_seq_scan(d, "t"),),
    ),
    lambda d: PostgresRefreshMaterializedViewStatement(
        d, MaterializedView(d, "mv"), hints=(hint_seq_scan(d, "mv"),)
    ),
]


class TestStatementHintCapability:
    """The support switch refuses hints on a dialect that does not spell them."""

    @pytest.mark.parametrize("builder", HINTED_STATEMENTS)
    def test_a_dialect_without_the_extension_refuses_hints(self, builder):
        # New enough to spell every statement below, so the refusal that fires
        # is the hint probe and not an earlier version gate.
        dialect = PostgresDialect(version=(16, 0, 0))
        assert dialect.supports_pg_hint_plan() is False
        with pytest.raises(UnsupportedFeatureError) as caught:
            builder(dialect).to_sql()
        assert "optimizer hints" in str(caught.value)
        assert "pg_hint_plan" in str(caught.value)

    def test_a_plain_statement_renders_without_the_extension(self):
        dialect = PostgresDialect(version=(16, 0, 0))
        query = QueryStatement(dialect, [Column(dialect, "id")], Table(dialect, "t"))
        assert query.to_sql() == ('SELECT "id" FROM "t"', ())

    def test_the_switch_is_the_extension_probe(self, dialect):
        assert dialect.supports_pg_hint_plan() is True


class TestUnaccent:
    def test_literal(self, dialect):
        assert unaccent(dialect, "Café").to_sql() == ("UNACCENT(%s)", ("Café",))

    def test_column(self, dialect):
        assert unaccent(dialect, Column(dialect, "name")).to_sql() == ('UNACCENT("name")', ())

    def test_with_dictionary(self, dialect):
        sql, params = unaccent(dialect, "Café", dictionary="unaccent").to_sql()
        assert sql == "UNACCENT(%s, %s)"
        assert params == ("unaccent", "Café")

    @pytest.mark.parametrize("dictionary", ["bad-name", "bad name", ""])
    def test_bad_dictionary_rejected(self, dialect, dictionary):
        with pytest.raises(ValueError):
            unaccent(dialect, "x", dictionary=dictionary)


class TestCapabilityGates:
    @pytest.mark.parametrize(
        ("name", "version", "supported"),
        [
            ("pg_hint_plan", "1.8.1", True),
            ("pg_hint_plan", "1.2.0", False),
            ("unaccent", "1.1", True),
        ],
    )
    def test_version_gates(self, name, version, supported):
        dialect = PostgresDialect()
        dialect._extensions = {
            name: PostgresExtensionInfo(name=name, installed=True, available=True, version=version)
        }
        if name == "pg_hint_plan":
            assert dialect.supports_pg_hint_plan() is supported
        else:
            assert dialect.supports_unaccent() is supported

    def test_not_installed(self):
        dialect = PostgresDialect()
        assert dialect.supports_pg_hint_plan() is False
        assert dialect.supports_unaccent() is False
