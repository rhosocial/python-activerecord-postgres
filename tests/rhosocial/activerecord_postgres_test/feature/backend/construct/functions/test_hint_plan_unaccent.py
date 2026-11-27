# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/functions/test_hint_plan_unaccent.py
"""SQL construction tests for pg_hint_plan and unaccent factories."""

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import Column, QueryStatement
from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.statements.dml import (
    DeleteStatement,
    InsertStatement,
    UpdateStatement,
)
from rhosocial.activerecord.backend.expression.statements.explain import ExplainStatement
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.hint import (
    CorrectionKind,
    HINT_FIELDS,
    HintGroup,
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

    A hinted statement renders only on a dialect whose ``supports_statement_hints()``
    answers true, which is the pg_hint_plan probe. The extension is simulated here
    because these are construction tests: no server is contacted.
    """
    dialect = PostgresDialect()
    dialect._extensions = {
        "pg_hint_plan": PostgresExtensionInfo(
            name="pg_hint_plan", installed=True, available=True, version="1.8.1"
        )
    }
    return dialect


@pytest.fixture
def query(dialect):
    return QueryStatement(dialect=dialect, select=[Column(dialect, "id")], from_=Table(dialect, "t"))


def _hinted_query(dialect, hint):
    """The smallest statement that renders one hint phrase."""
    return QueryStatement(
        dialect,
        select=[Column(dialect, "id")],
        from_=Table(dialect, "t"),
        hints=(hint,),
    )


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


class TestStatementHints:
    def test_query_prefix_and_params(self, dialect, query):
        expr = QueryStatement(
            dialect,
            select=[Column(dialect, "id")],
            from_=Table(dialect, "t"),
            hints=(hint_seq_scan(dialect, "t"),),
        )
        sql, params = expr.to_sql()
        assert sql == '/*+ SeqScan(t) */ SELECT "id" FROM "t"'
        assert params == ()

    def test_multiple_hints_share_one_comment(self, dialect):
        hints = [hint_leading(dialect, "a", "b"), hint_hash_join(dialect, "a", "b")]
        expr = QueryStatement(
            dialect, [Column(dialect, "id")], Table(dialect, "t"), hints=tuple(hints)
        )
        assert expr.to_sql()[0] == '/*+ Leading(a b) HashJoin(a b) */ SELECT "id" FROM "t"'

    def test_a_dict_hint_is_accepted(self, dialect):
        payload = hint_seq_scan(dialect, "t")
        expr = QueryStatement(
            dialect, [Column(dialect, "id")], Table(dialect, "t"), hints=(payload,)
        )
        assert expr.to_sql()[0] == '/*+ SeqScan(t) */ SELECT "id" FROM "t"'

    def test_insert_update_delete(self, dialect):
        table = Table(dialect, "t")
        inserts = InsertStatement(
            dialect,
            table,
            ValuesSource(dialect, [[Literal(dialect, 1)]]),
            ["a"],
            hints=(hint_seq_scan(dialect, "t"),),
        )
        updates = UpdateStatement(
            dialect,
            table,
            {"a": Literal(dialect, 1)},
            hints=(hint_parallel(dialect, "t", 4),),
        )
        deletes = DeleteStatement(
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

    def test_explain_prefix(self, dialect, query):
        expr = ExplainStatement(dialect, query, hints=(hint_seq_scan(dialect, "t"),))
        assert expr.to_sql() == ('/*+ SeqScan(t) */ EXPLAIN SELECT "id" FROM "t"', ())

    def test_explain_refuses_a_hinted_statement(self, dialect):
        hinted = QueryStatement(
            dialect, [Column(dialect, "id")], Table(dialect, "t"), hints=(hint_seq_scan(dialect, "t"),)
        )
        expr = ExplainStatement(dialect, hinted, hints=(hint_seq_scan(dialect, "t"),))
        with pytest.raises(TypeError):
            expr.to_sql()

    def test_explain_refuses_a_hinted_statement_without_its_own_hints(self, dialect):
        """Two comment positions cannot be resolved: the wrapped hints win the refusal."""
        hinted = QueryStatement(
            dialect, [Column(dialect, "id")], Table(dialect, "t"), hints=(hint_seq_scan(dialect, "t"),)
        )
        with pytest.raises(TypeError):
            ExplainStatement(dialect, hinted).to_sql()

    def test_load_statement(self, dialect):
        assert load_pg_hint_plan(dialect).to_sql() == ("LOAD 'pg_hint_plan'", ())

    def test_empty_hints_render_the_plain_statement(self, dialect, query):
        """An empty payload list is "no hints", not a refused statement."""
        expr = QueryStatement(
            dialect, [Column(dialect, "id")], Table(dialect, "t"), hints=()
        )
        assert expr.to_sql() == ('SELECT "id" FROM "t"', ())

    def test_non_hint_rejected(self, dialect):
        expr = QueryStatement(
            dialect, [Column(dialect, "id")], Table(dialect, "t"), hints=("SeqScan(t)",)
        )
        with pytest.raises(TypeError):
            expr.to_sql()

    def test_plain_statement_has_no_prefix(self, dialect, query):
        # The formatter override must not touch a statement that carries no hints.
        assert query.to_sql() == ('SELECT "id" FROM "t"', ())

    def test_explain_of_a_plain_statement_has_no_prefix(self, dialect, query):
        assert ExplainStatement(dialect, query).to_sql() == ('EXPLAIN SELECT "id" FROM "t"', ())


class TestStatementHintCapability:
    """The support switch refuses hints on a dialect that does not spell them."""

    def test_a_dialect_without_the_extension_refuses_hints(self):
        dialect = PostgresDialect()
        assert dialect.supports_statement_hints() is False
        expr = QueryStatement(
            dialect,
            [Column(dialect, "id")],
            Table(dialect, "t"),
            hints=(hint_seq_scan(dialect, "t"),),
        )
        with pytest.raises(UnsupportedFeatureError) as caught:
            expr.to_sql()
        assert "optimizer hints" in str(caught.value)

    def test_a_plain_statement_renders_without_the_extension(self):
        dialect = PostgresDialect()
        query = QueryStatement(dialect, [Column(dialect, "id")], Table(dialect, "t"))
        assert query.to_sql() == ('SELECT "id" FROM "t"', ())

    def test_the_switch_delegates_to_the_extension_probe(self, dialect):
        assert dialect.supports_statement_hints() is True
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
