# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/statements/test_partition_plan.py
"""Unit tests for PostgreSQL partition plans and their DDL expressions.

No server is required: interval arithmetic, policy bounds, tree derivation and
DDL rendering all run against a dialect and the plan's own specs.
"""

from datetime import date, datetime, timezone

import pytest

from rhosocial.activerecord.backend.expression.column_types.column import Column
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.statements import (
    DropTableStatement,
    PartitionClause,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.statements.ddl.partition import (
    PostgresCreatePartitionStatement,
    PostgresPartitionTreeQueryStatement,
)
from rhosocial.activerecord.backend.impl.postgres.partition import (
    HashBranch,
    HashPolicy,
    ListBranch,
    ListPolicy,
    PostgresPartitionPlan,
    RangeBranch,
    RangePolicy,
    add_interval,
    default_label,
    iter_range_bounds,
    normalize_interval,
)

UTC = timezone.utc


def _dt(year, month, day):
    return datetime(year, month, day, tzinfo=UTC)


def _multilevel_plan():
    p3_bounds = []
    for month in range(1, 13):
        first = _dt(2026, month, 1)
        mid = _dt(2026, month, 16)
        if month == 12:
            nxt = _dt(2027, 1, 1)
        else:
            nxt = _dt(2026, month + 1, 1)
        p3_bounds.append((first, mid))
        p3_bounds.append((mid, nxt))
    return PostgresPartitionPlan(
        parent="logs",
        method="HASH",
        columns=("tenant_bucket",),
        modulus=4,
        branches=(
            HashBranch(
                0,
                RangePolicy(
                    columns=("created_at",), start=_dt(2026, 1, 1), interval="month", count=12
                ),
            ),
            HashBranch(
                1,
                RangePolicy(
                    columns=("created_at",), start=_dt(2026, 1, 1), interval="quarter", count=4
                ),
            ),
            HashBranch(
                2,
                RangePolicy(
                    columns=("created_at",), start=_dt(2026, 1, 1), interval="year", count=2
                ),
            ),
            HashBranch(3, RangePolicy(columns=("created_at",), bounds=tuple(p3_bounds))),
        ),
    )


class TestIntervalArithmetic:
    def test_normalize_interval_accepts_units_pairs_and_spellings(self):
        assert normalize_interval("month") == (1, "month")
        assert normalize_interval("15 day") == (15, "day")
        assert normalize_interval((2, "quarter")) == (2, "quarter")

    def test_normalize_interval_rejects_unknown_unit(self):
        with pytest.raises(ValueError, match="interval unit"):
            normalize_interval("week")

    def test_add_interval_clamps_day_of_month(self):
        assert add_interval(date(2026, 1, 31), "month") == date(2026, 2, 28)
        assert add_interval(date(2026, 11, 30), "quarter") == date(2027, 2, 28)

    def test_add_interval_keeps_datetime_tz(self):
        value = _dt(2026, 12, 31)
        assert add_interval(value, "month") == _dt(2027, 1, 31)
        assert add_interval(value, (15, "day")) == _dt(2027, 1, 15)

    def test_monthly_ranges_keep_the_original_day_anchor(self):
        starts = [start for start, _ in iter_range_bounds(date(2026, 1, 31), "month", count=4)]
        assert starts == [
            date(2026, 1, 31),
            date(2026, 2, 28),
            date(2026, 3, 31),
            date(2026, 4, 30),
        ]

    def test_default_labels_by_granularity(self):
        assert default_label(_dt(2026, 1, 1), "month") == "2026_01"
        assert default_label(_dt(2026, 2, 1), "quarter") == "2026_q1"
        assert default_label(_dt(2026, 1, 1), "year") == "2026"
        assert default_label(_dt(2026, 1, 1), (15, "day")) == "2026_01_01"


class TestPolicyBounds:
    def test_monthly_bounds_are_half_open(self):
        policy = RangePolicy(
            columns=("created_at",), start=_dt(2026, 1, 1), interval="month", count=2
        )
        assert tuple(policy.iter_bounds()) == (
            (_dt(2026, 1, 1), _dt(2026, 2, 1)),
            (_dt(2026, 2, 1), _dt(2026, 3, 1)),
        )

    def test_quarterly_bounds(self):
        policy = RangePolicy(
            columns=("created_at",), start=_dt(2026, 1, 1), interval="quarter", count=4
        )
        bounds = tuple(policy.iter_bounds())
        assert bounds[0] == (_dt(2026, 1, 1), _dt(2026, 4, 1))
        assert bounds[-1] == (_dt(2026, 10, 1), _dt(2027, 1, 1))

    def test_explicit_bounds_win_over_interval(self):
        policy = RangePolicy(
            columns=("created_at",),
            bounds=((_dt(2026, 1, 1), _dt(2026, 1, 16)), (_dt(2026, 1, 16), _dt(2026, 2, 1))),
        )
        assert tuple(policy.iter_bounds())[1] == (_dt(2026, 1, 16), _dt(2026, 2, 1))

    def test_policy_requires_start_or_bounds(self):
        with pytest.raises(ValueError, match="requires start or bounds"):
            RangePolicy(columns=("created_at",))

    def test_generated_policy_requires_a_horizon(self):
        policy = RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1))
        with pytest.raises(ValueError, match="requires count or through"):
            tuple(policy.iter_bounds())

    def test_through_expands_each_interval_to_the_same_boundary(self):
        monthly = RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1), interval="month")
        quarterly = RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1), interval="quarter")
        yearly = RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1), interval="year")
        through = _dt(2027, 1, 1)
        assert len(tuple(monthly.iter_bounds(through=through))) == 12
        assert len(tuple(quarterly.iter_bounds(through=through))) == 4
        assert len(tuple(yearly.iter_bounds(through=through))) == 1

    def test_list_and_hash_policies(self):
        listed = ListPolicy(columns=("region",), groups=(("us", "ca"), ("eu",)))
        assert tuple(listed.iter_groups()) == (
            (("us", "ca"), "us_ca"),
            (("eu",), "eu"),
        )
        hashed = HashPolicy(columns=("bucket",), modulus=4)
        assert tuple(hashed.iter_remainders()) == (0, 1, 2, 3)


class TestPlanGeneration:
    def test_heterogeneous_tree_has_four_branches_and_forty_two_leaves(self):
        plan = _multilevel_plan()
        specs = tuple(plan.iter_specs())
        assert len(specs) == 46
        assert [spec.name for spec in specs if spec.level == 1] == [
            "logs_p0",
            "logs_p1",
            "logs_p2",
            "logs_p3",
        ]
        leaves = [spec for spec in specs if spec.level == 2]
        assert len(leaves) == 42
        assert {spec.parent for spec in leaves} == {
            "logs_p0",
            "logs_p1",
            "logs_p2",
            "logs_p3",
        }

    def test_branch_names_are_derived_not_written(self):
        plan = _multilevel_plan()
        names = [spec.name for spec in plan.iter_specs() if spec.level == 2]
        assert "logs_p0_2026_01" in names
        assert "logs_p1_2026_q1" in names
        assert "logs_p2_2026" in names
        assert "logs_p3_2026_01_16" in names
        assert "logs_p3_2026_12_16" in names

    def test_flat_plan_keeps_tracker_shape(self):
        plan = PostgresPartitionPlan(
            parent="job_logs",
            method="RANGE",
            columns=("attempt_created_at",),
            policy=RangePolicy(
                columns=("attempt_created_at",), start=_dt(2026, 9, 1), interval="month", count=2
            ),
        )
        specs = tuple(plan.iter_specs())
        assert [spec.name for spec in specs] == [
            "job_logs_2026_09",
            "job_logs_2026_10",
        ]
        assert all(spec.level == 1 and spec.is_leaf for spec in specs)

    def test_branch_name_suffix_composes_with_child_leaves(self):
        plan = PostgresPartitionPlan(
            parent="logs",
            method="HASH",
            columns=("bucket",),
            modulus=1,
            branches=(
                HashBranch(
                    0,
                    RangePolicy(
                        columns=("created_at",), start=_dt(2026, 1, 1), interval="month", count=1
                    ),
                    name="cold",
                ),
            ),
        )
        specs = tuple(plan.iter_specs())
        assert [spec.name for spec in specs] == ["logs_cold", "logs_cold_2026_01"]

    def test_name_pattern_is_the_only_naming_hook(self):
        plan = PostgresPartitionPlan(
            parent="logs",
            method="RANGE",
            columns=("created_at",),
            policy=RangePolicy(
                columns=("created_at",), start=_dt(2026, 1, 1), interval="month", count=1
            ),
            name_pattern="{parent}__{suffix}",
        )
        assert tuple(plan.iter_specs())[0].name == "logs__2026_01"

    def test_branches_and_policy_are_mutually_exclusive(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            PostgresPartitionPlan(
                parent="logs",
                method="RANGE",
                columns=("created_at",),
                policy=RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1)),
                branches=(RangeBranch(_dt(2026, 1, 1), _dt(2027, 1, 1)),),
            )

    def test_flat_policy_must_match_parent_method(self):
        with pytest.raises(ValueError, match="policy must be a RangePolicy"):
            PostgresPartitionPlan(
                parent="logs",
                method="RANGE",
                columns=("created_at",),
                policy=HashPolicy(columns=("bucket",), modulus=2),
            )

    def test_heterogeneous_child_policies_are_allowed(self):
        plan = PostgresPartitionPlan(
            parent="logs",
            method="HASH",
            columns=("bucket",),
            modulus=2,
            branches=(
                HashBranch(
                    0,
                    RangePolicy(
                        columns=("created_at",), start=_dt(2026, 1, 1), interval="month", count=1
                    ),
                ),
                HashBranch(1, ListPolicy(columns=("region",), groups=(("eu",),))),
            ),
        )
        assert [spec.method for spec in plan.iter_specs()][3] == "LIST"

    def test_duplicate_generated_names_are_rejected(self):
        plan = PostgresPartitionPlan(
            parent="logs",
            method="HASH",
            columns=("bucket",),
            modulus=4,
            branches=(HashBranch(0), HashBranch(0)),
        )
        with pytest.raises(ValueError, match="duplicate partition name"):
            tuple(plan.iter_specs())

    def test_hash_branch_requires_modulus(self):
        with pytest.raises(ValueError, match="requires modulus"):
            PostgresPartitionPlan(
                parent="logs",
                method="HASH",
                columns=("bucket",),
                branches=(HashBranch(0),),
            )

    def test_list_branch_only_for_list_parent(self):
        with pytest.raises(ValueError, match="RANGE parent's branches"):
            PostgresPartitionPlan(
                parent="logs",
                method="RANGE",
                columns=("created_at",),
                branches=(ListBranch(("eu",)),),
            )


class TestDdlRendering:
    """Rendering is owned by the DDL expression classes, driven by a plan."""

    @staticmethod
    def _dialect():
        return PostgresDialect(version=(17, 0, 0))

    def _create_statement(self, plan, spec, if_not_exists):
        dialect = self._dialect()
        clause = None
        if spec.child_method is not None:
            clause = PartitionClause(
                dialect,
                spec.child_method,
                [Column(dialect, name) for name in spec.child_columns],
            )
        statement = PostgresCreatePartitionStatement(
            dialect=dialect,
            partition_name=spec.name,
            parent_table=spec.parent,
            partition_type=spec.method,
            partition_values=dict(spec.bounds),
            schema=plan.schema,
            parent_schema=plan.schema,
            if_not_exists=if_not_exists,
            partition_clause=clause,
        )
        return statement.to_sql()

    def test_branch_render_appends_child_partition_clause(self):
        plan = _multilevel_plan()
        branch = next(plan.iter_specs())
        sql, params = self._create_statement(plan, branch, if_not_exists=True)
        assert sql == (
            'CREATE TABLE IF NOT EXISTS "logs_p0" PARTITION OF "logs" '
            "FOR VALUES WITH (MODULUS 4, REMAINDER 0) "
            'PARTITION BY RANGE ("created_at")'
        )
        assert params == ()

    def test_leaf_render_is_range_of_utc_bounds(self):
        plan = _multilevel_plan()
        leaf = tuple(plan.iter_specs())[1]
        sql, params = self._create_statement(plan, leaf, if_not_exists=False)
        assert sql == (
            'CREATE TABLE "logs_p0_2026_01" PARTITION OF "logs_p0" '
            "FOR VALUES FROM ('2026-01-01 00:00:00+00:00') "
            "TO ('2026-02-01 00:00:00+00:00')"
        )

    def test_drop_render_never_cascades_by_default(self):
        plan = _multilevel_plan()
        statement = DropTableStatement(
            dialect=self._dialect(),
            table=Table(self._dialect(), "logs_p0_2026_01", schema_name=plan.schema),
            if_exists=True,
            cascade=False,
        )
        sql, _ = statement.to_sql()
        assert sql == 'DROP TABLE IF EXISTS "logs_p0_2026_01"'

    def test_tree_query_is_a_single_parameter_expression(self):
        plan = _multilevel_plan()
        statement = PostgresPartitionTreeQueryStatement(
            self._dialect(),
            root_table=plan.parent,
            schema=plan.schema,
            include_stats=True,
        )
        sql, params = statement.to_sql()
        assert "WITH RECURSIVE partition_tree" in sql
        assert "pg_inherits" in sql
        assert "pg_get_expr(cls.relpartbound" in sql
        assert sql.count("%s") == 1
        assert params == ("logs",)
