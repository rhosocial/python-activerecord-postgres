# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/partition/test_partition_offline.py
"""Offline unit tests for PostgreSQL partition maintenance and plan helpers.

Every check here is synchronous and connectionless: pure parsing/comparison
helpers are called directly, plan derivation is exercised in memory, and the
catalog/DDL I/O seams are replaced with ``MagicMock``. No PostgreSQL server,
network or driver connection is involved.
"""

from dataclasses import FrozenInstanceError
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.partition import (
    HashBranch,
    HashPolicy,
    ListBranch,
    ListPolicy,
    PairedDifference,
    PartitionBatchReport,
    PartitionInfo,
    PartitionReport,
    PartitionSpec,
    PostgresPartitionMaintenance,
    PostgresPartitionPlan,
    RangeBranch,
    RangePolicy,
)
from rhosocial.activerecord.backend.impl.postgres.partition import maintenance as mnt
from rhosocial.activerecord.backend.impl.postgres.partition import plan as plan_mod

UTC = timezone.utc
PLUS8 = timezone(timedelta(hours=8))
PLUS830 = timezone(timedelta(hours=8, minutes=30))
MINUS5 = timezone(-timedelta(hours=5))


def _info(name, **overrides):
    values = {"name": name, "parent": "events", "level": 1}
    values.update(overrides)
    return PartitionInfo(**values)


def _spec(name, method="RANGE", **overrides):
    values = {
        "name": name,
        "parent": "events",
        "method": method,
        "columns": ("created_at",),
        "bounds": {},
        "level": 1,
        "is_leaf": True,
    }
    values.update(overrides)
    return PartitionSpec(**values)


def _range_plan(parent="events", bounds=None):
    if bounds is None:
        bounds = ((date(2026, 1, 1), date(2026, 2, 1)),)
    return PostgresPartitionPlan(
        parent=parent,
        method="RANGE",
        columns="created_at",
        policy=RangePolicy(bounds=bounds),
    )


def _base():
    backend = MagicMock()
    backend.dialect = PostgresDialect(version=(17, 0, 0))
    return mnt._PartitionMaintenanceBase(backend), backend


# --------------------------------------------------------------------------- #
# Pure bound-text helpers
# --------------------------------------------------------------------------- #
class TestUnquoteBoundValue:
    @pytest.mark.parametrize(
        "text, expected",
        [
            ("'abc'", "abc"),
            ("  'abc'  ", "abc"),
            ("'a''b'", "a'b"),
            ("''", ""),
            ("abc", "abc"),
            ("", ""),
            ("'unterminated", "'unterminated"),
            ("'  x  '", "  x  "),
        ],
    )
    def test_unquote(self, text, expected):
        assert mnt._unquote_bound_value(text) == expected


class TestSplitBoundValues:
    @pytest.mark.parametrize(
        "text, expected",
        [
            ("a, b, c", ("a", "b", "c")),
            ("  a  ,  b  ", ("a", "b")),
            ("'a,b', c", ("'a,b'", "c")),
            ("'a''b', c", ("'a''b'", "c")),
            ("'x,y','z'", ("'x,y'", "'z'")),
            ("", ()),
            ("a", ("a",)),
            ("a,,b", ("a", "", "b")),
            ("a,", ("a",)),
        ],
    )
    def test_split(self, text, expected):
        assert mnt._split_bound_values(text) == expected


class TestParseListValue:
    @pytest.mark.parametrize(
        "token, expected",
        [
            ("'abc'", "abc"),
            ("'a''b'", "a'b"),
            ("'NULL'", "NULL"),
            ("NULL", None),
            ("null", None),
            ("42", 42),
            ("-7", -7),
            ("+3", 3),
            ("  42  ", 42),
            ("1.5", 1.5),
            ("-0.25", -0.25),
            ("abc", "abc"),
            ("", ""),
        ],
    )
    def test_parse(self, token, expected):
        assert mnt._parse_list_value(token) == expected


class TestParseTemporal:
    @pytest.mark.parametrize(
        "text, expected",
        [
            ("2026-01-02", date(2026, 1, 2)),
            ("'2026-01-02'", date(2026, 1, 2)),
            ("2026-01-02 03:04", datetime(2026, 1, 2, 3, 4)),
            ("2026-01-02T03:04:05", datetime(2026, 1, 2, 3, 4, 5)),
            ("2026-01-02 03:04:05.12", datetime(2026, 1, 2, 3, 4, 5, 120000)),
            ("2026-01-02 03:04:05Z", datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)),
            ("2026-01-02 03:04:05+08", datetime(2026, 1, 2, 3, 4, 5, tzinfo=PLUS8)),
            ("2026-01-02 03:04:05+08:30", datetime(2026, 1, 2, 3, 4, 5, tzinfo=PLUS830)),
            ("2026-01-02 03:04:05-05:00", datetime(2026, 1, 2, 3, 4, 5, tzinfo=MINUS5)),
            ("MINVALUE", None),
            ("MAXVALUE", None),
            ("'MAXVALUE'", None),
            ("NULL", None),
            ("nonsense", None),
            ("2026-13-01", None),
            ("2026-02-30", None),
            ("2026-01-02 03:04:05.1234567", None),
            ("", None),
        ],
    )
    def test_parse(self, text, expected):
        assert mnt._parse_temporal(text) == expected


class TestParsePartitionBound:
    @pytest.mark.parametrize("text", [None, "", "   ", 123, "nonsense"])
    def test_unparseable_returns_none(self, text):
        assert mnt._parse_partition_bound(text) is None

    def test_range_date_bounds(self):
        result = mnt._parse_partition_bound("FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')")
        assert result == {
            "kind": "RANGE",
            "range_start": date(2026, 1, 1),
            "range_end": date(2026, 2, 1),
        }

    def test_range_with_maxvalue_omits_unparsed_end(self):
        result = mnt._parse_partition_bound("for values from ('2026-01-01') to (maxvalue)")
        assert result == {"kind": "RANGE", "range_start": date(2026, 1, 1)}

    def test_list_values(self):
        result = mnt._parse_partition_bound("FOR VALUES IN ('a', 'b', 3, NULL)")
        assert result == {"kind": "LIST", "list_values": ("a", "b", 3, None)}

    @pytest.mark.parametrize(
        "text, modulus, remainder",
        [
            ("FOR VALUES WITH (MODULUS 4, REMAINDER 2)", 4, 2),
            ("for values with (modulus 8, remainder 0)", 8, 0),
            ("FOR VALUES WITH ( modulus 3 , remainder 1 )", 3, 1),
        ],
    )
    def test_hash_bounds(self, text, modulus, remainder):
        assert mnt._parse_partition_bound(text) == {
            "kind": "HASH",
            "modulus": modulus,
            "remainder": remainder,
        }


class TestSameBound:
    @pytest.mark.parametrize(
        "actual, expected, same",
        [
            (date(2026, 1, 1), date(2026, 1, 1), True),
            (date(2026, 1, 1), date(2026, 1, 2), False),
            (datetime(2026, 1, 1), datetime(2026, 1, 1), True),
            (datetime(2026, 1, 1, 12), datetime(2026, 1, 1), False),
            (date(2026, 1, 1), datetime(2026, 1, 1), True),
            (datetime(2026, 1, 1), date(2026, 1, 1), True),
            (date(2026, 1, 1), datetime(2026, 1, 1, 12), False),
            (datetime(2026, 1, 1, 8, tzinfo=PLUS8), datetime(2026, 1, 1, tzinfo=UTC), True),
            (datetime(2026, 1, 1, 9, tzinfo=PLUS8), datetime(2026, 1, 1, tzinfo=UTC), False),
            (datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 1), True),
            ("a", "a", True),
            (1, 1, True),
            (1, 2, False),
            (None, None, True),
            (None, "x", False),
        ],
    )
    def test_same_bound(self, actual, expected, same):
        assert mnt._same_bound(actual, expected) is same


class TestFormatAndNormalise:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (datetime(2026, 1, 2, 3, 4, 5), "2026-01-02 03:04:05"),
            (date(2026, 1, 2), "2026-01-02"),
            (42, "42"),
            (None, "None"),
        ],
    )
    def test_format_bound_value(self, value, expected):
        assert mnt._format_bound_value(value) == expected

    @pytest.mark.parametrize(
        "text, expected",
        [
            ("  a   b  ", "a b"),
            ("\n a\t b \n", "a b"),
            ("", None),
            ("   ", None),
            (None, None),
            (123, None),
        ],
    )
    def test_normalise_bound_text(self, text, expected):
        assert mnt._normalise_bound_text(text) == expected


class TestKeyValueAndSorting:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (datetime(2026, 1, 1), "2026-01-01"),
            (datetime(2026, 1, 1, tzinfo=UTC), "2026-01-01"),
            (datetime(2026, 1, 1, 12, 30), "2026-01-01 12:30:00"),
            (date(2026, 1, 1), "2026-01-01"),
            ("abc", "'abc'"),
            (4, "4"),
            (None, "None"),
        ],
    )
    def test_key_value(self, value, expected):
        assert mnt._key_value(value) == expected

    @pytest.mark.parametrize(
        "values, expected",
        [
            ((3, 1, 2), (1, 2, 3)),
            (("b", "a"), ("a", "b")),
            (("a", 1), (1, "a")),
            ((), ()),
        ],
    )
    def test_sorted_values(self, values, expected):
        assert mnt._sorted_values(values) == expected


class TestExtrasBucket:
    def test_range_bucket(self):
        info = _info(
            "p",
            method="range",
            extras={"range_start": date(2026, 1, 1), "range_end": date(2026, 2, 1)},
        )
        assert mnt._extras_bucket(info) == "RANGE|2026-01-01|2026-02-01"

    def test_range_bucket_normalises_midnight_datetime(self):
        info = _info(
            "p",
            method="RANGE",
            extras={
                "range_start": datetime(2026, 1, 1),
                "range_end": datetime(2026, 2, 1),
            },
        )
        assert mnt._extras_bucket(info) == "RANGE|2026-01-01|2026-02-01"

    def test_hash_bucket(self):
        info = _info("p", method="HASH", extras={"modulus": 4, "remainder": 2})
        assert mnt._extras_bucket(info) == "HASH|4|2"

    def test_list_bucket_sorts_values(self):
        info = _info("p", method="LIST", extras={"list_values": ("b", "a")})
        assert mnt._extras_bucket(info) == "LIST|'a','b'"

    def test_list_bucket_with_integers(self):
        info = _info("p", method="LIST", extras={"list_values": (2, 1)})
        assert mnt._extras_bucket(info) == "LIST|1,2"

    def test_incomplete_or_missing_extras_return_none(self):
        assert mnt._extras_bucket(_info("p", method="RANGE", extras={"range_start": 1})) is None
        assert mnt._extras_bucket(_info("p", method="LIST", extras={})) is None
        assert (
            mnt._extras_bucket(_info("p", method=None, extras={"modulus": 4, "remainder": 1}))
            is None
        )


class TestDifferenceKeyAndSignature:
    def test_extras_take_precedence_over_bound(self):
        info = _info(
            "events_2026_01",
            method="RANGE",
            bound="some unparsed text",
            extras={"range_start": date(2026, 1, 1), "range_end": date(2026, 2, 1)},
        )
        assert mnt._difference_key(info) == "RANGE|2026-01-01|2026-02-01"
        assert mnt._boundary_signature(info) == "RANGE|2026-01-01|2026-02-01"

    def test_bound_is_fallback(self):
        info = _info("events_x", bound="  FOR   VALUES IN ('a')  ")
        assert mnt._difference_key(info) == "FOR VALUES IN ('a')"
        assert mnt._boundary_signature(info) == "FOR VALUES IN ('a')"

    def test_name_suffix_is_last_resort(self):
        info = _info("events_2026_01", parent="events")
        assert mnt._difference_key(info) == "2026_01"

    def test_unprefixed_name_used_as_is(self):
        assert mnt._difference_key(_info("orphan", parent="events")) == "orphan"
        assert mnt._difference_key(_info("solo", parent=None)) == "solo"

    def test_signature_none_without_extras_or_bound(self):
        assert mnt._boundary_signature(_info("p")) is None

    def test_boundaries_disagree_on_verbatim_text(self):
        left = _info("p", bound="for values in ('a')")
        right = _info("p", bound="FOR VALUES IN ('a')")
        assert mnt._boundaries_disagree(left, right) is True

    def test_boundaries_agree_on_same_text(self):
        left = _info("p", bound="FOR VALUES IN ('a')")
        right = _info("p", bound="  FOR VALUES IN ('a')  ")
        assert mnt._boundaries_disagree(left, right) is False

    def test_boundaries_disagree_falls_back_to_extras(self):
        extras = {"range_start": date(2026, 1, 1), "range_end": date(2026, 2, 1)}
        left = _info("p", method="RANGE", extras=extras)
        right = _info(
            "p",
            method="RANGE",
            extras={"range_start": date(2026, 1, 1), "range_end": date(2026, 3, 1)},
        )
        assert mnt._boundaries_disagree(left, right) is True
        assert mnt._boundaries_disagree(left, _info("p", method="RANGE", extras=extras)) is False

    def test_boundaries_agree_when_both_have_no_signature(self):
        assert mnt._boundaries_disagree(_info("a"), _info("b")) is False


# --------------------------------------------------------------------------- #
# Shape derivation helpers
# --------------------------------------------------------------------------- #
class TestShapeHelpers:
    def test_policy_shape_none(self):
        assert mnt._policy_shape(None) is None

    def test_policy_shape_explicit_range(self):
        policy = RangePolicy(columns="d", bounds=((date(2026, 1, 1), date(2026, 2, 1)),))
        assert mnt._policy_shape(policy) == ("RANGE", ("d",), None, True)

    @pytest.mark.parametrize(
        "interval, normalised",
        [
            ("quarter", (1, "quarter")),
            ((3, "month"), (3, "month")),
            ("15 day", (15, "day")),
        ],
    )
    def test_policy_shape_generated_range(self, interval, normalised):
        policy = RangePolicy(columns="d", start=date(2026, 1, 1), interval=interval)
        assert mnt._policy_shape(policy) == ("RANGE", ("d",), normalised, False)

    def test_policy_shape_list(self):
        policy = ListPolicy(columns="status", groups=(("a",),))
        assert mnt._policy_shape(policy) == ("LIST", ("status",))

    def test_policy_shape_hash(self):
        assert mnt._policy_shape(HashPolicy(columns="tenant", modulus=4)) == (
            "HASH",
            ("tenant",),
            4,
        )

    def test_branch_shape(self):
        assert mnt._branch_shape(HashBranch(remainder=0)) == ("HashBranch", None)
        branch = HashBranch(remainder=1, child=HashPolicy(modulus=2))
        assert mnt._branch_shape(branch) == ("HashBranch", ("HASH", ("bucket",), 2))
        branch = RangeBranch(date(2026, 1, 1), date(2026, 2, 1))
        assert mnt._branch_shape(branch) == ("RangeBranch", None)

    def test_plan_shape_equal_for_identical_plans(self):
        left = _range_plan()
        right = _range_plan()
        assert mnt._plan_shape(left) == mnt._plan_shape(right)

    @pytest.mark.parametrize(
        "mutate",
        [
            lambda p: PostgresPartitionPlan(
                parent=p.parent,
                method="LIST",
                columns=p.columns,
                policy=ListPolicy(groups=(("a",),)),
            ),
            lambda p: PostgresPartitionPlan(
                parent=p.parent, method="RANGE", columns="other", policy=p.policy
            ),
            lambda p: PostgresPartitionPlan(
                parent=p.parent,
                method="RANGE",
                columns=p.columns,
                policy=RangePolicy(start=date(2027, 1, 1), interval="year"),
            ),
        ],
    )
    def test_plan_shape_detects_structure_changes(self, mutate):
        plan = _range_plan()
        assert mnt._plan_shape(mutate(plan)) != mnt._plan_shape(plan)

    def test_plan_shape_includes_branches(self):
        plan = PostgresPartitionPlan(
            parent="events",
            method="HASH",
            columns="tenant",
            modulus=2,
            branches=(
                HashBranch(0),
                HashBranch(1, child=RangePolicy(bounds=((date(2026, 1, 1), date(2026, 2, 1)),))),
            ),
        )
        shape = mnt._plan_shape(plan)
        assert shape[0] == "HASH"
        assert shape[2] == 2
        assert shape[4] == (
            ("HashBranch", None),
            ("HashBranch", ("RANGE", ("created_at",), None, True)),
        )


# --------------------------------------------------------------------------- #
# _PartitionMaintenanceBase helpers
# --------------------------------------------------------------------------- #
class TestParseRows:
    def test_empty_rows(self):
        assert mnt._PartitionMaintenanceBase._parse_rows([]) == ()

    def test_derives_method_bound_extras_and_stats(self):
        rows = [
            {
                "name": "events_2026_01",
                "parent": "events",
                "level": 1,
                "schema_name": "public",
                "partition_key": "RANGE (created_at)",
                "bound": "FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')",
                "row_count": "12",
                "total_bytes": "4096",
                "is_partitioned": 0,
            }
        ]
        info = mnt._PartitionMaintenanceBase._parse_rows(rows)[0]
        assert info.name == "events_2026_01"
        assert info.parent == "events"
        assert info.level == 1
        assert info.schema == "public"
        assert info.method == "RANGE"
        assert info.bound == rows[0]["bound"]
        assert info.row_count == 12
        assert info.size_bytes == 4096
        assert info.is_partitioned is False
        assert info.extras == {
            "range_start": date(2026, 1, 1),
            "range_end": date(2026, 2, 1),
        }

    @pytest.mark.parametrize(
        "partition_key, method",
        [
            ("HASH (tenant)", "HASH"),
            ("LIST (status)", "LIST"),
            ("RANGE (created_at)", "RANGE"),
            ("KEY (x)", None),
            (None, None),
            ("", None),
        ],
    )
    def test_method_detection(self, partition_key, method):
        info = mnt._PartitionMaintenanceBase._parse_rows(
            [
                {"name": "p", "partition_key": partition_key},
            ]
        )[0]
        assert info.method == method

    def test_missing_stats_become_none_and_defaults(self):
        info = mnt._PartitionMaintenanceBase._parse_rows([{"name": "p"}])[0]
        assert info.row_count is None
        assert info.size_bytes is None
        assert info.level == 0
        assert info.parent is None
        assert info.is_partitioned is False
        assert info.extras == {}

    def test_list_extras_parsed(self):
        info = mnt._PartitionMaintenanceBase._parse_rows(
            [
                {"name": "p", "partition_key": "LIST (status)", "bound": "FOR VALUES IN ('a', 2)"},
            ]
        )[0]
        assert info.extras == {"list_values": ("a", 2)}


class TestExpectedHelpers:
    def test_expected_returns_specs(self):
        base, _ = _base()
        specs = base._expected(_range_plan(), None)
        assert [spec.name for spec in specs] == ["events_2026_01_01"]

    def test_expected_rejects_plan_without_partitions(self):
        base, _ = _base()
        plan = PostgresPartitionPlan(parent="events", method="RANGE", columns="d")
        with pytest.raises(ValueError, match="no partitions"):
            base._expected(plan, None)

    def test_safe_expected_returns_empty_for_unbounded_plan(self):
        base, _ = _base()
        plan = PostgresPartitionPlan(
            parent="events",
            method="RANGE",
            columns="d",
            policy=RangePolicy(start=date(2026, 1, 1)),
        )
        assert base._safe_expected(plan) == ()
        assert len(base._safe_expected(plan, through=date(2026, 3, 1))) == 2

    def test_safe_expected_reraises_for_bounded_empty_plan(self):
        base, _ = _base()
        plan = PostgresPartitionPlan(parent="events", method="RANGE", columns="d")
        with pytest.raises(ValueError):
            base._safe_expected(plan)

    def test_actual_names_excludes_root(self):
        infos = [
            _info("events", level=0, parent=None),
            _info("p1", level=1),
            _info("p1_a", level=2, parent="p1"),
        ]
        actual = mnt._PartitionMaintenanceBase._actual_names(infos)
        assert set(actual) == {"p1", "p1_a"}
        assert actual["p1"].level == 1


class TestBoundaryComparison:
    def _range_spec(self):
        return _spec(
            "events_2026_01_01",
            method="RANGE",
            bounds={"from": date(2026, 1, 1), "to": date(2026, 2, 1)},
            range_start=date(2026, 1, 1),
            range_end=date(2026, 2, 1),
        )

    def test_range_matching_bound_has_no_issue(self):
        base, _ = _base()
        info = _info("events_2026_01_01", bound="FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')")
        assert base._boundary_issue(self._range_spec(), info) is None
        assert base._boundary_mismatch(self._range_spec(), info) is False

    def test_range_mismatch_issue_and_flag(self):
        base, _ = _base()
        info = _info("events_2026_01_01", bound="FOR VALUES FROM ('2026-01-01') TO ('2026-03-01')")
        assert base._boundary_issue(self._range_spec(), info) == (
            "boundary mismatch for events_2026_01_01: catalog "
            "2026-01-01..2026-03-01, plan 2026-01-01..2026-02-01"
        )
        assert base._boundary_mismatch(self._range_spec(), info) is True

    def test_unparsed_bound_issue_but_no_mismatch(self):
        base, _ = _base()
        info = _info("events_2026_01_01", bound=None)
        assert base._boundary_issue(self._range_spec(), info) == (
            "unparsed bound for events_2026_01_01: <missing>"
        )
        assert base._boundary_mismatch(self._range_spec(), info) is False

    def test_maxvalue_bound_is_unparsed_issue(self):
        base, _ = _base()
        info = _info("events_2026_01_01", bound="FOR VALUES FROM ('2026-01-01') TO (MAXVALUE)")
        assert base._boundary_issue(self._range_spec(), info).startswith("unparsed bound")

    def test_kind_mismatch(self):
        base, _ = _base()
        info = _info("events_2026_01_01", bound="FOR VALUES IN ('a')")
        assert base._boundary_issue(self._range_spec(), info) == (
            "boundary mismatch for events_2026_01_01: catalog LIST bound, plan RANGE bound"
        )
        assert base._boundary_mismatch(self._range_spec(), info) is True

    def test_list_boundary_comparison(self):
        base, _ = _base()
        spec = _spec("events_a", method="LIST", bounds={"values": ("a", "b")})
        matching = _info("events_a", bound="FOR VALUES IN ('b', 'a')")
        assert base._boundary_issue(spec, matching) is None
        assert base._boundary_mismatch(spec, matching) is False
        wrong = _info("events_a", bound="FOR VALUES IN ('x')")
        assert base._boundary_issue(spec, wrong) == (
            "boundary mismatch for events_a: catalog values ('x',), plan values ('a', 'b')"
        )
        assert base._boundary_mismatch(spec, wrong) is True

    def test_hash_boundary_comparison(self):
        base, _ = _base()
        spec = _spec("events_p2", method="HASH", bounds={"modulus": 4, "remainder": 2})
        matching = _info("events_p2", bound="FOR VALUES WITH (MODULUS 4, REMAINDER 2)")
        assert base._boundary_issue(spec, matching) is None
        wrong = _info("events_p2", bound="FOR VALUES WITH (MODULUS 4, REMAINDER 1)")
        assert base._boundary_issue(spec, wrong) == (
            "boundary mismatch for events_p2: catalog modulus 4 remainder 1, "
            "plan modulus 4 remainder 2"
        )
        assert base._boundary_mismatch(spec, wrong) is True

    def test_needs_partition_missing_or_mismatched(self):
        base, _ = _base()
        spec = self._range_spec()
        assert base._needs_partition(spec, {}) is True
        matching = _info(spec.name, bound="FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')")
        assert base._needs_partition(spec, {spec.name: matching}) is False
        wrong = _info(spec.name, bound="FOR VALUES FROM ('2026-01-01') TO ('2026-03-01')")
        assert base._needs_partition(spec, {spec.name: wrong}) is True

    def test_missing_pairs(self):
        base, _ = _base()
        spec = self._range_spec()
        assert base._missing([spec], {}) == (("events", spec.name),)
        matching = _info(spec.name, bound="FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')")
        assert base._missing([spec], {spec.name: matching}) == ()


class TestOtherBaseHelpers:
    def test_orphans_bounded(self):
        base, _ = _base()
        plan = _range_plan()
        spec = _spec("events_2026_01_01")
        actual = {"events_2026_01_01": _info("events_2026_01_01"), "events_z": _info("events_z")}
        assert base._orphans(plan, [spec], actual, None) == ("events_z",)

    def test_orphans_unbounded(self):
        base, _ = _base()
        plan = PostgresPartitionPlan(
            parent="events",
            method="RANGE",
            columns="d",
            policy=RangePolicy(start=date(2026, 1, 1)),
        )
        assert base._orphans(plan, [], {"x": _info("x")}, None) == ()

    def test_units_flat_plan(self):
        specs = [_spec("a"), _spec("b")]
        assert mnt._PartitionMaintenanceBase._units(_range_plan(), specs) == (tuple(specs),)
        assert mnt._PartitionMaintenanceBase._units(_range_plan(), []) == ()

    def test_units_multilevel_plan(self):
        plan = PostgresPartitionPlan(
            parent="events",
            method="HASH",
            columns="tenant",
            modulus=2,
            branches=(HashBranch(0), HashBranch(1)),
        )
        branch0 = _spec("events_p0", level=1)
        leaf0 = _spec("events_p0_0", level=2, parent="events_p0")
        branch1 = _spec("events_p1", level=1)
        assert mnt._PartitionMaintenanceBase._units(plan, [branch0, leaf0, branch1]) == (
            (branch0, leaf0),
            (branch1,),
        )

    def test_continuity_gap_and_overlap(self):
        base, _ = _base()
        specs = [
            _spec("a", range_start=date(2026, 1, 1), range_end=date(2026, 2, 1)),
            _spec("b", range_start=date(2026, 2, 2), range_end=date(2026, 3, 1)),
            _spec("c", range_start=date(2026, 2, 15), range_end=date(2026, 3, 15), parent="other"),
        ]
        issues = base._continuity_issues(specs)
        assert "gap in events between a and b" in issues
        assert "overlap in other between b and c" not in issues  # b is in a different parent

    def test_continuity_overlap(self):
        base, _ = _base()
        specs = [
            _spec("a", range_start=date(2026, 1, 1), range_end=date(2026, 2, 1)),
            _spec("b", range_start=date(2026, 1, 15), range_end=date(2026, 2, 15)),
        ]
        assert base._continuity_issues(specs) == ("overlap in events between a and b",)

    def test_continuity_ignores_unranged_specs(self):
        base, _ = _base()
        assert base._continuity_issues([_spec("a"), _spec("b")]) == ()

    def test_structure_issues_clean(self):
        base, _ = _base()
        plan = _range_plan()
        root = _info("events", level=0, parent=None, method="RANGE")
        spec = _spec(
            "events_2026_01_01",
            method="RANGE",
            bounds={"from": date(2026, 1, 1), "to": date(2026, 2, 1)},
            range_start=date(2026, 1, 1),
            range_end=date(2026, 2, 1),
        )
        info = _info(spec.name, bound="FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')")
        assert base._structure_issues(plan, [root, info], [spec]) == ()

    def test_structure_issues_no_root(self):
        base, _ = _base()
        assert base._structure_issues(_range_plan(), [], []) == (
            "partitioned parent events does not exist",
        )

    def test_structure_issues_root_method_mismatch(self):
        base, _ = _base()
        root = _info("events", level=0, parent=None, method="LIST")
        assert base._structure_issues(_range_plan(), [root], []) == (
            "parent events is partitioned by LIST, plan says RANGE",
        )

    def test_structure_issues_parent_and_leaf_flags(self):
        base, _ = _base()
        root = _info("events", level=0, parent=None, method="RANGE")
        spec = _spec(
            "p",
            bounds={"from": date(2026, 1, 1), "to": date(2026, 2, 1)},
            range_start=date(2026, 1, 1),
            range_end=date(2026, 2, 1),
        )
        info = _info(
            "p",
            parent="other",
            is_partitioned=True,
            bound="FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')",
        )
        issues = base._structure_issues(_range_plan(), [root, info], [spec])
        assert "p belongs to other, plan expects events" in issues
        assert "leaf p is partitioned; plan expects it to be a leaf" in issues

    def test_structure_issues_branch_flags(self):
        base, _ = _base()
        root = _info("events", level=0, parent=None, method="RANGE")
        spec = _spec("p", child_method="RANGE")
        plain = _info("p", is_partitioned=False)
        issues = base._structure_issues(_range_plan(), [root, plain], [spec])
        assert "branch p is not further partitioned; RANGE leaves are missing" in issues
        wrong = _info("p", is_partitioned=True, method="HASH")
        issues = base._structure_issues(_range_plan(), [root, wrong], [spec])
        assert "branch p is partitioned by HASH, plan says RANGE" in issues

    def test_report_composition(self):
        base, _ = _base()
        plan = _range_plan()
        infos = (_info("events", level=0, parent=None, method="RANGE"),)
        specs = (_spec("events_2026_01_01"),)
        report = base._report(plan, infos, specs, None, dry_run=True)
        assert report.target == "events"
        assert report.partitions == infos
        assert report.missing == (("events", "events_2026_01_01"),)
        assert report.issues == ()
        assert report.dry_run is True

    def test_without(self):
        specs = [_spec("a"), _spec("b"), _spec("c")]
        assert mnt._PartitionMaintenanceBase._without(specs, ["b"]) == (specs[0], specs[2])

    def test_drop_candidates_rejects_non_date_cutoff(self):
        base, _ = _base()
        with pytest.raises(TypeError, match="cutoff must be a date or datetime"):
            base._drop_candidates(_range_plan(), "2026-02-15", [], None)

    def test_drop_candidates_selects_early_existing_leaves(self):
        base, _ = _base()
        plan = _range_plan(
            bounds=(
                (date(2026, 1, 1), date(2026, 2, 1)),
                (date(2026, 2, 1), date(2026, 3, 1)),
            )
        )
        infos = [
            _info("events_2026_01_01", bound="FOR VALUES FROM ('2026-01-01') " "TO ('2026-02-01')"),
            _info("events_2026_02_01", bound="FOR VALUES FROM ('2026-02-01') " "TO ('2026-03-01')"),
        ]
        candidates = base._drop_candidates(plan, date(2026, 2, 15), infos, None)
        assert [spec.name for spec in candidates] == ["events_2026_01_01"]

    def test_resolve_batch_dedup_and_pairs(self):
        plan_a = _range_plan("a")
        plan_b = _range_plan("b")
        entries, unique = mnt._PartitionMaintenanceBase._resolve_batch(
            [plan_a, plan_a, plan_b], None
        )
        assert entries == ()
        assert unique == (plan_a, plan_b)
        entries, unique = mnt._PartitionMaintenanceBase._resolve_batch(
            None, [("pair", plan_a, plan_b)]
        )
        assert entries == (("pair", plan_a, plan_b),)
        assert unique == (plan_a, plan_b)

    @pytest.mark.parametrize(
        "plans, pairs, message",
        [
            ([], None, "at least one plan"),
            (["x"], None, "must be PostgresPartitionPlan"),
            (None, [("bad",)], "must be \\(name, left_plan, right_plan\\)"),
        ],
    )
    def test_resolve_batch_errors(self, plans, pairs, message):
        with pytest.raises((TypeError, ValueError), match=message):
            mnt._PartitionMaintenanceBase._resolve_batch(plans, pairs)

    def test_bucket_map_flags_duplicates(self):
        infos = [
            _info("a", level=1, bound="FOR VALUES IN ('x')"),
            _info("b", level=1, bound="FOR VALUES IN ('x')"),
            _info("skip", level=2, bound="FOR VALUES IN ('y')"),
        ]
        issues = []
        buckets = mnt._PartitionMaintenanceBase._bucket_map("p", "left", infos, issues)
        assert set(buckets) == {"FOR VALUES IN ('x')"}
        assert "duplicate bucket" in issues[0]

    def test_paired_report_differences_and_issues(self):
        base, _ = _base()
        left_plan = _range_plan("left_t")
        right_plan = _range_plan("right_t")
        left_report = PartitionReport(
            target="left_t",
            partitions=(_info("l_only", level=1, bound="FOR VALUES IN ('left')"),),
        )
        right_report = PartitionReport(
            target="right_t",
            partitions=(_info("r_only", level=1, bound="FOR VALUES IN ('right')"),),
        )
        differences, issues = base._paired_report(
            (("p", left_plan, right_plan),),
            {"left_t": left_report, "right_t": right_report},
        )
        assert issues == ()
        assert differences == (
            PairedDifference("p", "FOR VALUES IN ('left')", "left", "l_only", None),
            PairedDifference("p", "FOR VALUES IN ('right')", "right", None, "r_only"),
        )

    def test_paired_report_incompatible_plan(self):
        base, _ = _base()
        left_plan = _range_plan("left_t")
        right_plan = PostgresPartitionPlan(
            parent="right_t",
            method="LIST",
            columns="d",
            policy=ListPolicy(groups=(("a",),)),
        )
        differences, issues = base._paired_report((("p", left_plan, right_plan),), {})
        assert differences == ()
        assert issues == ("pair p plans are not comparable",)

    def test_paired_report_boundary_drift_with_same_semantic_key(self):
        base, _ = _base()
        left_plan = _range_plan("t_left")
        right_plan = _range_plan("t_right")
        extras = {"range_start": date(2026, 1, 1), "range_end": date(2026, 2, 1)}
        left = _info(
            "l",
            level=1,
            method="RANGE",
            extras=extras,
            bound="FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')",
        )
        right = _info(
            "r",
            level=1,
            method="RANGE",
            extras=extras,
            bound="for values from ('2026-01-01') to ('2026-02-01')",
        )
        reports = {
            "t_left": PartitionReport(target="t_left", partitions=(left,)),
            "t_right": PartitionReport(target="t_right", partitions=(right,)),
        }
        differences, issues = base._paired_report((("p", left_plan, right_plan),), reports)
        assert differences == ()
        assert issues == ("boundary mismatch for p/RANGE|2026-01-01|2026-02-01",)

    def test_batch_report_assembly(self):
        reports = {"t": PartitionReport(target="t")}
        batch = mnt._PartitionMaintenanceBase._batch_report(
            (("p", _range_plan("a"), _range_plan("b")),),
            reports,
            (PairedDifference("p", "k", "left", "l", None),),
            ("issue",),
        )
        assert isinstance(batch, PartitionBatchReport)
        assert batch.reports == reports
        assert batch.pairs == ("p",)
        assert batch.issues == ("issue",)
        assert len(batch.differences) == 1


# --------------------------------------------------------------------------- #
# I/O seams (mocked)
# --------------------------------------------------------------------------- #
class TestIoSeams:
    def test_base_seams_raise_not_implemented(self):
        base, _ = _base()
        with pytest.raises(NotImplementedError):
            base._fetch_rows("SELECT 1", ())
        with pytest.raises(NotImplementedError):
            base._execute("DDL", ())
        with pytest.raises(NotImplementedError):
            base._run_units(_range_plan(), (), lambda spec: None)

    def test_sync_fetch_and_execute_delegate_to_backend(self):
        backend = MagicMock()
        backend.dialect = PostgresDialect(version=(17, 0, 0))
        backend.fetch_all.return_value = [{"name": "p"}]
        maintenance = PostgresPartitionMaintenance(backend)
        assert maintenance._fetch_rows("SELECT", ("p",)) == [{"name": "p"}]
        backend.fetch_all.assert_called_once_with("SELECT", ("p",))
        maintenance._execute("DDL", ("p",))
        backend.execute.assert_called_once_with("DDL", ("p",))

    def test_infos_uses_fetch_seam_and_parses_rows(self):
        base, _ = _base()
        rows = [
            {
                "name": "events_2026_01_01",
                "parent": "events",
                "level": 1,
                "partition_key": "RANGE (created_at)",
                "bound": "FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')",
            }
        ]
        base._fetch_rows = MagicMock(return_value=rows)
        infos = base._infos(_range_plan())
        assert base._fetch_rows.call_count == 1
        sql, params = base._fetch_rows.call_args.args
        assert "partition_tree" in sql
        assert params == ("events",)
        assert infos[0].name == "events_2026_01_01"
        assert "range_start" in infos[0].extras


# --------------------------------------------------------------------------- #
# plan.py pure helpers and policy objects
# --------------------------------------------------------------------------- #
class TestPlanModuleHelpers:
    @pytest.mark.parametrize(
        "value, expected",
        [
            ("a", ("a",)),
            (("a", "b"), ("a", "b")),
            (["a", "b"], ("a", "b")),
        ],
    )
    def test_as_columns_valid(self, value, expected):
        assert plan_mod._as_columns(value) == expected

    @pytest.mark.parametrize("value", [None, 1, {}])
    def test_as_columns_wrong_type(self, value):
        with pytest.raises(TypeError, match="columns must be a string"):
            plan_mod._as_columns(value)

    @pytest.mark.parametrize("value", [(), [], ("",), ("a", "  ")])
    def test_as_columns_empty(self, value):
        with pytest.raises(ValueError):
            plan_mod._as_columns(value)

    @pytest.mark.parametrize(
        "method, expected",
        [
            ("range", "RANGE"),
            ("List", "LIST"),
            ("HASH", "HASH"),
        ],
    )
    def test_normalise_method(self, method, expected):
        assert plan_mod._normalise_method(method) == expected

    @pytest.mark.parametrize("method", ["KEY", "", None, 1])
    def test_normalise_method_rejects_unknown(self, method):
        with pytest.raises(ValueError, match="method must be one of"):
            plan_mod._normalise_method(method)

    @pytest.mark.parametrize(
        "values, expected",
        [
            (("a", "b"), "a_b"),
            ((1, 2), "1_2"),
            (("a-b",), "a_b"),
            (("",), "value"),
        ],
    )
    def test_label_from_values(self, values, expected):
        assert plan_mod._label_from_values(values) == expected

    def test_render_label(self):
        assert (
            plan_mod._render_label("{year}-{month:02d}", date(2026, 3, 1), date(2026, 4, 1))
            == "2026-03"
        )
        assert plan_mod._render_label("q{quarter}", date(2026, 4, 1), date(2026, 7, 1)) == "q2"


class TestRangePolicy:
    def test_method_and_explicit_bounds(self):
        policy = RangePolicy(columns="d", bounds=((date(2026, 1, 1), date(2026, 2, 1)),))
        assert policy.method == "RANGE"
        assert list(policy.iter_bounds()) == [(date(2026, 1, 1), date(2026, 2, 1))]
        assert policy.label_for(date(2026, 1, 1), date(2026, 2, 1)) == "2026_01_01"

    def test_generated_bounds_by_count(self):
        policy = RangePolicy(start=date(2026, 1, 1), count=3)
        assert list(policy.iter_bounds()) == [
            (date(2026, 1, 1), date(2026, 2, 1)),
            (date(2026, 2, 1), date(2026, 3, 1)),
            (date(2026, 3, 1), date(2026, 4, 1)),
        ]
        assert policy.label_for(date(2026, 1, 1), date(2026, 2, 1)) == "2026_01"

    def test_generated_bounds_by_through(self):
        policy = RangePolicy(start=date(2026, 1, 1), interval=(15, "day"))
        assert list(policy.iter_bounds(through=date(2026, 1, 20))) == [
            (date(2026, 1, 1), date(2026, 1, 16)),
            (date(2026, 1, 16), date(2026, 1, 31)),
        ]

    def test_generated_policy_without_horizon_raises(self):
        policy = RangePolicy(start=date(2026, 1, 1))
        with pytest.raises(ValueError, match="count or through"):
            list(policy.iter_bounds())

    def test_custom_label_pattern(self):
        policy = RangePolicy(
            start=date(2026, 1, 1),
            count=1,
            label_pattern="y{year}q{quarter}",
        )
        assert policy.label_for(date(2026, 1, 1), date(2026, 4, 1)) == "y2026q1"

    @pytest.mark.parametrize(
        "kwargs",
        [
            {},
            {"start": date(2026, 1, 1), "bounds": ((date(2026, 1, 1), date(2026, 2, 1)),)},
            {"start": "2026-01-01"},
            {"start": date(2026, 1, 1), "count": 0},
            {"start": date(2026, 1, 1), "count": True},
            {"start": date(2026, 1, 1), "interval": "fortnight"},
            {"bounds": ()},
            {"bounds": ((date(2026, 1, 1),),)},
            {"bounds": ((date(2026, 2, 1), date(2026, 1, 1)),)},
        ],
    )
    def test_invalid_construction(self, kwargs):
        with pytest.raises((TypeError, ValueError)):
            RangePolicy(**kwargs)


class TestListPolicy:
    def test_iter_groups_derived_labels(self):
        policy = ListPolicy(columns="status", groups=(("a", "b"), ("c",)))
        assert policy.method == "LIST"
        assert list(policy.iter_groups()) == [(("a", "b"), "a_b"), (("c",), "c")]

    def test_iter_groups_explicit_labels(self):
        policy = ListPolicy(groups=(("a",), ("b",)), labels=("first", "second"))
        assert list(policy.iter_groups()) == [(("a",), "first"), (("b",), "second")]

    @pytest.mark.parametrize(
        "kwargs",
        [
            {},
            {"groups": ((),)},
            {"groups": (("a",),), "labels": ("one", "two")},
            {"groups": (("a",),), "labels": ("",)},
        ],
    )
    def test_invalid_construction(self, kwargs):
        with pytest.raises(ValueError):
            ListPolicy(**kwargs)


class TestHashPolicy:
    def test_all_remainders(self):
        policy = HashPolicy(modulus=3)
        assert policy.method == "HASH"
        assert list(policy.iter_remainders()) == [0, 1, 2]

    def test_selected_remainders(self):
        policy = HashPolicy(modulus=4, remainders=(2, 0))
        assert list(policy.iter_remainders()) == [2, 0]

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"modulus": 0},
            {"modulus": -1},
            {"modulus": True},
            {"modulus": "4"},
            {"modulus": 4, "remainders": ()},
            {"modulus": 4, "remainders": (4,)},
            {"modulus": 4, "remainders": (1, 1)},
            {"modulus": 4, "remainders": (True,)},
        ],
    )
    def test_invalid_construction(self, kwargs):
        with pytest.raises((TypeError, ValueError)):
            HashPolicy(**kwargs)


class TestBranches:
    def test_hash_branch_defaults(self):
        branch = HashBranch(remainder=3)
        assert branch.remainder == 3
        assert branch.child is None
        assert branch.name is None

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"remainder": "0"},
            {"remainder": True},
            {"remainder": -1},
            {"remainder": 0, "name": "  "},
        ],
    )
    def test_hash_branch_invalid(self, kwargs):
        with pytest.raises((TypeError, ValueError)):
            HashBranch(**kwargs)

    def test_range_branch(self):
        branch = RangeBranch(date(2026, 1, 1), date(2026, 2, 1), label="jan")
        assert branch.label == "jan"

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"start": "2026-01-01", "end": date(2026, 2, 1)},
            {"start": date(2026, 2, 1), "end": date(2026, 1, 1)},
            {"start": date(2026, 1, 1), "end": date(2026, 1, 1), "name": "  "},
            {"start": date(2026, 1, 1), "end": date(2026, 2, 1), "label": ""},
        ],
    )
    def test_range_branch_invalid(self, kwargs):
        with pytest.raises((TypeError, ValueError)):
            RangeBranch(**kwargs)

    def test_list_branch_normalises_values(self):
        assert ListBranch(values=["a", "b"]).values == ("a", "b")

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"values": ()},
            {"values": ("a",), "name": ""},
            {"values": ("a",), "label": " "},
        ],
    )
    def test_list_branch_invalid(self, kwargs):
        with pytest.raises(ValueError):
            ListBranch(**kwargs)


class TestPostgresPartitionPlan:
    def test_method_and_columns_normalised(self):
        plan = PostgresPartitionPlan(
            parent="events",
            method="range",
            columns="d",
            policy=RangePolicy(count=1, start=date(2026, 1, 1)),
        )
        assert plan.method == "RANGE"
        assert plan.columns == ("d",)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"parent": ""},
            {"parent": "e", "method": "KEY"},
            {"parent": "e", "method": "RANGE", "columns": ()},
            {"parent": "e", "method": "RANGE", "name_pattern": "x"},
            {"parent": "e", "method": "RANGE", "name_pattern": ""},
            {"parent": "e", "method": "RANGE", "schema": "  "},
        ],
    )
    def test_invalid_plan_fields(self, kwargs):
        with pytest.raises((TypeError, ValueError)):
            PostgresPartitionPlan(**kwargs)

    def test_policy_and_branches_mutually_exclusive(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            PostgresPartitionPlan(
                parent="e",
                method="HASH",
                columns="tenant",
                modulus=2,
                branches=(HashBranch(0),),
                policy=HashPolicy(modulus=2),
            )

    @pytest.mark.parametrize(
        "kwargs, message",
        [
            (
                {
                    "parent": "e",
                    "method": "HASH",
                    "columns": "tenant",
                    "policy": RangePolicy(bounds=((date(2026, 1, 1), date(2026, 2, 1)),)),
                },
                "must be a HashPolicy",
            ),
            (
                {
                    "parent": "e",
                    "method": "HASH",
                    "columns": "tenant",
                    "branches": (HashBranch(0),),
                },
                "requires modulus",
            ),
            (
                {"parent": "e", "method": "RANGE", "columns": "d", "modulus": 2},
                "only meaningful for a HASH parent",
            ),
            (
                {
                    "parent": "e",
                    "method": "HASH",
                    "columns": "tenant",
                    "modulus": 0,
                    "branches": (HashBranch(0),),
                },
                "positive integer",
            ),
            (
                {
                    "parent": "e",
                    "method": "HASH",
                    "columns": "tenant",
                    "modulus": 1,
                    "branches": (HashBranch(1),),
                },
                "less than modulus",
            ),
        ],
    )
    def test_hash_specific_validation(self, kwargs, message):
        with pytest.raises((TypeError, ValueError), match=message):
            PostgresPartitionPlan(**kwargs)

    @pytest.mark.parametrize(
        "method, branch",
        [
            ("HASH", RangeBranch(date(2026, 1, 1), date(2026, 2, 1))),
            ("RANGE", HashBranch(0)),
            ("LIST", HashBranch(0)),
        ],
    )
    def test_branch_type_must_match_method(self, method, branch):
        with pytest.raises(ValueError, match="branches must be"):
            PostgresPartitionPlan(parent="e", method=method, columns="d", branches=(branch,))

    def test_branch_suffix_variants(self):
        plan = PostgresPartitionPlan(
            parent="events",
            method="HASH",
            columns="tenant",
            modulus=2,
            branches=(HashBranch(0), HashBranch(1, name="custom")),
        )
        assert plan.branch_suffix(0, plan.branches[0]) == "p0"
        assert plan.branch_suffix(1, plan.branches[1]) == "custom"
        assert plan.branch_name(0, plan.branches[0]) == "events_p0"
        assert plan.branch_name(1, plan.branches[1]) == "events_custom"

    def test_branch_suffix_range_and_list(self):
        range_branch = RangeBranch(date(2026, 3, 1), date(2026, 4, 1), label="march")
        list_branch = ListBranch(values=("a", "b"))
        assert (
            plan_mod.PostgresPartitionPlan(
                parent="e", method="RANGE", columns="d", branches=(range_branch,)
            ).branch_suffix(0, range_branch)
            == "march"
        )
        assert (
            plan_mod.PostgresPartitionPlan(
                parent="e", method="LIST", columns="d", branches=(list_branch,)
            ).branch_suffix(0, list_branch)
            == "a_b"
        )

    def test_custom_name_pattern(self):
        plan = PostgresPartitionPlan(
            parent="events",
            method="HASH",
            columns="tenant",
            modulus=2,
            branches=(HashBranch(0),),
            name_pattern="{parent}~{suffix}",
        )
        assert plan.branch_name(0, plan.branches[0]) == "events~p0"

    def test_iter_specs_flat(self):
        plan = _range_plan(
            bounds=(
                (date(2026, 1, 1), date(2026, 2, 1)),
                (date(2026, 2, 1), date(2026, 3, 1)),
            )
        )
        specs = list(plan.iter_specs())
        assert [spec.name for spec in specs] == [
            "events_2026_01_01",
            "events_2026_02_01",
        ]
        assert all(spec.level == 1 and spec.is_leaf for spec in specs)

    def test_iter_specs_multilevel_order(self):
        plan = PostgresPartitionPlan(
            parent="events",
            method="HASH",
            columns="tenant",
            modulus=2,
            branches=(
                HashBranch(0, child=RangePolicy(bounds=((date(2026, 1, 1), date(2026, 2, 1)),))),
                HashBranch(1),
            ),
        )
        specs = list(plan.iter_specs())
        assert [spec.level for spec in specs] == [1, 2, 1]
        assert specs[0].name == "events_p0"
        assert specs[0].child_method == "RANGE"
        assert specs[1].name == "events_p0_2026_01_01"
        assert specs[1].parent == "events_p0"
        assert specs[2].is_leaf is True

    def test_iter_specs_duplicate_name_raises(self):
        plan = PostgresPartitionPlan(
            parent="events",
            method="HASH",
            columns="tenant",
            modulus=2,
            branches=(HashBranch(0, name="same"), HashBranch(1, name="same")),
        )
        with pytest.raises(ValueError, match="duplicate partition name"):
            list(plan.iter_specs())

    @pytest.mark.parametrize(
        "plan, through, bounded",
        [
            (_range_plan(), None, True),
            (
                PostgresPartitionPlan(
                    parent="e",
                    method="RANGE",
                    columns="d",
                    policy=RangePolicy(start=date(2026, 1, 1)),
                ),
                None,
                False,
            ),
            (
                PostgresPartitionPlan(
                    parent="e",
                    method="RANGE",
                    columns="d",
                    policy=RangePolicy(start=date(2026, 1, 1)),
                ),
                date(2026, 3, 1),
                True,
            ),
            (PostgresPartitionPlan(parent="e", method="RANGE", columns="d"), None, True),
        ],
    )
    def test_is_fully_bounded(self, plan, through, bounded):
        assert plan.is_fully_bounded(through=through) is bounded


class TestPartitionSpec:
    def test_fields_and_equality(self):
        spec = _spec(
            "p",
            range_start=date(2026, 1, 1),
            range_end=date(2026, 2, 1),
            child_method="HASH",
            child_columns=("tenant",),
            label="jan",
        )
        assert spec.name == "p"
        assert spec.parent == "events"
        assert spec.child_columns == ("tenant",)
        assert spec == _spec(
            "p",
            range_start=date(2026, 1, 1),
            range_end=date(2026, 2, 1),
            child_method="HASH",
            child_columns=("tenant",),
            label="jan",
        )

    def test_frozen(self):
        spec = _spec("p")
        with pytest.raises(FrozenInstanceError):
            spec.name = "other"
