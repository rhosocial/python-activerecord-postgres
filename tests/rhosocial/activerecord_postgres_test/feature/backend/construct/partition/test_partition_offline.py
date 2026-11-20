# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/partition/test_partition_offline.py
"""Offline unit tests for the PostgreSQL partition plan grammar.

Everything here is connectionless: plan helpers, policies, branches and
``PostgresPartitionPlan`` derivation are exercised in memory. No PostgreSQL
server, network or driver connection is involved.
"""

from dataclasses import FrozenInstanceError
from datetime import date

import pytest

from rhosocial.activerecord.backend.impl.postgres.partition import (
    HashBranch,
    HashPolicy,
    ListBranch,
    ListPolicy,
    PartitionSpec,
    PostgresPartitionPlan,
    RangeBranch,
    RangePolicy,
)
from rhosocial.activerecord.backend.impl.postgres.partition import plan as plan_mod


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
