# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/functions/test_hint_plan_vocabulary.py
"""The full pg_hint_plan vocabulary: phrases, payloads and version gates."""

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.expression.objects import Table, View
from rhosocial.activerecord.backend.expression.sources import NamedRelationRef
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.hint import HINT_FIELDS, hint_payload
from rhosocial.activerecord.backend.impl.postgres.expression.hinted_statements import (
    PostgresQueryStatement,
)
from rhosocial.activerecord.backend.impl.postgres.functions import (
    hint_bitmap_scan_regexp,
    hint_disable_index,
    hint_index_only_scan_regexp,
    hint_index_scan_regexp,
    hint_memoize,
    hint_no_bitmap_scan,
    hint_no_hash_join,
    hint_no_index_only_scan,
    hint_no_memoize,
    hint_no_merge_join,
    hint_no_nest_loop,
    hint_no_tid_scan,
)
from rhosocial.activerecord.backend.impl.postgres.protocols.base import PostgresExtensionInfo


def _dialect(server=(18, 7, 0), extension="2.0.0"):
    dialect = PostgresDialect(version=server)
    dialect._extensions = {
        "pg_hint_plan": PostgresExtensionInfo(
            name="pg_hint_plan", installed=True, available=True, version=extension
        )
    }
    return dialect


@pytest.fixture
def dialect():
    """A server and extension that satisfy every gate below."""
    return _dialect()


def _phrase(dialect, hint):
    expr = PostgresQueryStatement(
        dialect,
        select=[Column(dialect, "id")],
        from_=Table(dialect, "t"),
        hints=(hint,),
    )
    sql, _ = expr.to_sql()
    return sql.split(" */ ")[0][len("/*+ ") :]


#: One case per newly added HintKind: builder, phrase, kind, tables, indexes.
#: The builder's first target is overridable so the target-resolution cases can
#: pass a ``NamedRelationRef`` through the same call.
NEW_HINTS = [
    (
        lambda d, t="t": hint_index_scan_regexp(d, t, "^idx_.*"),
        "IndexScanRegexp(t ^idx_.*)",
        "IndexScanRegexp",
        ("t",),
        ("^idx_.*",),
    ),
    (
        lambda d, t="t": hint_index_only_scan_regexp(d, t, "^i_", "_only$"),
        "IndexOnlyScanRegexp(t ^i_ _only$)",
        "IndexOnlyScanRegexp",
        ("t",),
        ("^i_", "_only$"),
    ),
    (
        lambda d, t="t": hint_bitmap_scan_regexp(d, t, "^bm_.*"),
        "BitmapScanRegexp(t ^bm_.*)",
        "BitmapScanRegexp",
        ("t",),
        ("^bm_.*",),
    ),
    (
        lambda d, t="t": hint_no_tid_scan(d, t),
        "NoTidScan(t)",
        "NoTidScan",
        ("t",),
        (),
    ),
    (
        lambda d, t="t": hint_no_index_only_scan(d, t),
        "NoIndexOnlyScan(t)",
        "NoIndexOnlyScan",
        ("t",),
        (),
    ),
    (
        lambda d, t="t": hint_no_bitmap_scan(d, t),
        "NoBitmapScan(t)",
        "NoBitmapScan",
        ("t",),
        (),
    ),
    (
        lambda d, t="t": hint_disable_index(d, t, "idx1", "idx2"),
        "DisableIndex(t idx1 idx2)",
        "DisableIndex",
        ("t",),
        ("idx1", "idx2"),
    ),
    (
        lambda d, t="a": hint_no_nest_loop(d, t, "b"),
        "NoNestLoop(a b)",
        "NoNestLoop",
        ("a", "b"),
        (),
    ),
    (
        lambda d, t="a": hint_no_hash_join(d, t, "b"),
        "NoHashJoin(a b)",
        "NoHashJoin",
        ("a", "b"),
        (),
    ),
    (
        lambda d, t="a": hint_no_merge_join(d, t, "b", "c"),
        "NoMergeJoin(a b c)",
        "NoMergeJoin",
        ("a", "b", "c"),
        (),
    ),
    (
        lambda d, t="a": hint_memoize(d, t, "b"),
        "Memoize(a b)",
        "Memoize",
        ("a", "b"),
        (),
    ),
    (
        lambda d, t="a": hint_no_memoize(d, t, "b"),
        "NoMemoize(a b)",
        "NoMemoize",
        ("a", "b"),
        (),
    ),
]


class TestNewHintKinds:
    @pytest.mark.parametrize(("build", "phrase", "kind", "tables", "indexes"), NEW_HINTS)
    def test_phrase_and_payload_round_trip(self, dialect, build, phrase, kind, tables, indexes):
        payload = build(dialect)
        assert _phrase(dialect, payload) == phrase
        assert payload["kind"] == kind
        assert payload["tables"] == tables
        assert payload["indexes"] == indexes
        assert set(payload) == set(HINT_FIELDS)
        # Artifacts decode arrays as lists; a rebuilt payload renders as-is.
        decoded = dict(payload, tables=list(payload["tables"]), indexes=list(payload["indexes"]))
        assert hint_payload(decoded) == payload
        assert _phrase(dialect, decoded) == phrase

    @pytest.mark.parametrize(("build", "phrase", "kind", "tables", "indexes"), NEW_HINTS)
    def test_targets_go_through_hint_target(self, dialect, build, phrase, kind, tables, indexes):
        """A relation target resolves alias-first with no schema qualification."""
        table = Table(dialect, "inner_name", schema_name="sales", name_need_quote=False)
        ref = NamedRelationRef(dialect, table, alias="a", alias_need_quote=False)
        payload = build(dialect, ref)
        assert payload["tables"][0] == "a"
        assert "sales" not in payload["tables"][0]

    @pytest.mark.parametrize(("build", "phrase", "kind", "tables", "indexes"), NEW_HINTS)
    def test_a_view_target_is_refused(self, dialect, build, phrase, kind, tables, indexes):
        """Refusal happens in the builder's resolver, never in a statement."""
        view = View(dialect, "v")
        with pytest.raises(TypeError, match="view"):
            build(dialect, view)


class TestVersionGates:
    @pytest.mark.parametrize(
        "build", [lambda d: hint_memoize(d, "a"), lambda d: hint_no_memoize(d, "a")]
    )
    def test_memoize_needs_postgresql_17(self, build):
        old = _dialect(server=(16, 4, 0), extension="2.0.0")
        payload = build(old)  # a payload may always be built
        with pytest.raises(UnsupportedFeatureError) as caught:
            _phrase(old, payload)
        assert "PostgreSQL 17" in str(caught.value)
        assert "16.4.0" in str(caught.value)

    @pytest.mark.parametrize(
        "build", [lambda d: hint_memoize(d, "a"), lambda d: hint_no_memoize(d, "a")]
    )
    def test_memoize_renders_on_17_and_later(self, build):
        new = _dialect(server=(17, 0, 0), extension="2.0.0")
        assert _phrase(new, build(new)) in ("Memoize(a)", "NoMemoize(a)")

    def test_disable_index_needs_pg_hint_plan_2(self):
        old = _dialect(server=(18, 7, 0), extension="1.8.1")
        payload = hint_disable_index(old, "t", "idx")
        with pytest.raises(UnsupportedFeatureError) as caught:
            _phrase(old, payload)
        assert "2.0" in str(caught.value)
        assert "1.8.1" in str(caught.value)

    def test_disable_index_renders_on_extension_2(self):
        new = _dialect(server=(18, 7, 0), extension="2.0.0")
        assert _phrase(new, hint_disable_index(new, "t", "idx")) == "DisableIndex(t idx)"

    def test_the_other_new_kinds_do_not_need_extension_2(self):
        old = _dialect(server=(18, 7, 0), extension="1.8.1")
        assert _phrase(old, hint_no_tid_scan(old, "t")) == "NoTidScan(t)"
        assert _phrase(old, hint_index_scan_regexp(old, "t", "^i")) == "IndexScanRegexp(t ^i)"
