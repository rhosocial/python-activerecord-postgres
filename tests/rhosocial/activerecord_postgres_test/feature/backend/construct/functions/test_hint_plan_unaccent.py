# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/functions/test_hint_plan_unaccent.py
"""SQL construction tests for pg_hint_plan and unaccent factories."""

import pytest

from rhosocial.activerecord.backend.expression import Column, QueryStatement
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.hint import (
    CorrectionKind,
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
    pg_hint_plan_hinted_query,
)
from rhosocial.activerecord.backend.impl.postgres.functions.unaccent import unaccent
from rhosocial.activerecord.backend.impl.postgres.protocols.base import PostgresExtensionInfo


@pytest.fixture
def dialect():
    return PostgresDialect()


@pytest.fixture
def query(dialect):
    return QueryStatement(dialect=dialect, select=[Column(dialect, "id")], from_=Table(dialect, "t"))


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
        assert factory(dialect).to_sql() == (expected, ())

    def test_nested_leading(self, dialect):
        hint = hint_leading(dialect, HintGroup("a", "b"), "c")
        assert hint.to_sql() == ("Leading((a b) c)", ())

    @pytest.mark.parametrize("table", ["t;DROP TABLE t", "a b", "", "1bad"])
    def test_untrusted_identifiers_rejected(self, dialect, table):
        # The constructor collects; the renderer refuses the identifier.
        hint = hint_seq_scan(dialect, table)
        with pytest.raises((ValueError, TypeError)):
            hint.to_sql()

    @pytest.mark.parametrize("amount", ["100", None, True])
    def test_bad_rows_amount_rejected(self, dialect, amount):
        hint = hint_rows(dialect, CorrectionKind.ABSOLUTE, amount, "a")
        with pytest.raises((ValueError, TypeError)):
            hint.to_sql()

    @pytest.mark.parametrize("guc,value", [("bad-name", "1"), ("valid", "1;2")])
    def test_bad_guc_rejected(self, dialect, guc, value):
        hint = hint_set(dialect, guc, value)
        with pytest.raises((ValueError, TypeError)):
            hint.to_sql()


class TestHintedQuery:
    def test_prefix_and_params(self, dialect, query):
        expr = pg_hint_plan_hinted_query(dialect, query, [hint_seq_scan(dialect, "t")])
        sql, params = expr.to_sql()
        assert sql == '/*+ SeqScan(t) */ SELECT "id" FROM "t"'
        assert params == ()

    def test_dialect_helper(self, dialect, query):
        hints = [hint_leading(dialect, "a", "b"), hint_hash_join(dialect, "a", "b")]
        expr = dialect.build_hinted_query(query, hints)
        assert expr.to_sql()[0] == '/*+ Leading(a b) HashJoin(a b) */ SELECT "id" FROM "t"'

    def test_load_statement(self, dialect):
        assert load_pg_hint_plan(dialect).to_sql() == ("LOAD 'pg_hint_plan'", ())

    def test_empty_hints_rejected(self, dialect, query):
        expr = dialect.build_hinted_query(query, [])
        with pytest.raises((ValueError, TypeError)):
            expr.to_sql()

    def test_non_hint_rejected(self, dialect, query):
        expr = dialect.build_hinted_query(query, ["SeqScan(t)"])
        with pytest.raises(TypeError):
            expr.to_sql()


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
