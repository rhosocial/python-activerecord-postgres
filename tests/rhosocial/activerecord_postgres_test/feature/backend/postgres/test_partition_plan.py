# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_partition_plan.py
"""Unit tests for PostgreSQL partition plans and maintenance planning.

No server is required. Plan generation, naming, DDL rendering and catalog
comparison all run against a dialect and a scripted backend, so the arithmetic
and the report logic are pinned without a live PostgreSQL scenario.
"""

from datetime import date, datetime, timezone

import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.partition import (
    AsyncPartitionEnsurer,
    AsyncPartitionInventory,
    AsyncPartitionRetention,
    AsyncPartitionValidator,
    PartitionEnsurer,
    PartitionInfo as CorePartitionInfo,
    PartitionInventory,
    PartitionReport as CorePartitionReport,
    PartitionRetention,
    PartitionValidator,
)
from rhosocial.activerecord.backend.impl.postgres.partition import (
    AsyncPostgresPartitionMaintenance,
    HashBranch,
    HashPolicy,
    ListBranch,
    ListPolicy,
    PartitionInfo,
    PartitionReport,
    PostgresPartitionMaintenance,
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


def _catalog_row(name, parent, level, partition_key=None, bound=None,
                 is_partitioned=False, row_count=0, total_bytes=0, schema="public"):
    return {
        "name": name,
        "parent": parent,
        "level": level,
        "schema_name": schema,
        "partition_key": partition_key,
        "bound": bound,
        "row_count": row_count,
        "total_bytes": total_bytes,
        "is_partitioned": is_partitioned,
    }


class _FakeTransaction:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False


class _FakeBackend:
    """Scripted backend: a fixed catalog plus DDL capture."""

    def __init__(self, rows, version=(17, 0, 0)):
        self.dialect = PostgresDialect(version=version)
        self.rows = list(rows)
        self.executed = []
        self.transaction_count = 0

    def fetch_all(self, sql, params=None):
        return list(self.rows)

    def execute(self, sql, params=None):
        self.executed.append((sql, tuple(params or ())))

    def transaction(self):
        self.transaction_count += 1
        return _FakeTransaction()


class _FakeAsyncTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        return False


class _FakeAsyncBackend(_FakeBackend):
    """Async scripted backend with the same catalog and DDL capture."""

    async def fetch_all(self, sql, params=None):
        return list(self.rows)

    async def execute(self, sql, params=None):
        self.executed.append((sql, tuple(params or ())))

    def transaction(self):
        self.transaction_count += 1
        return _FakeAsyncTransaction()


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
            HashBranch(0, RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1),
                                      interval="month", count=12)),
            HashBranch(1, RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1),
                                      interval="quarter", count=4)),
            HashBranch(2, RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1),
                                      interval="year", count=2)),
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
        starts = [
            start
            for start, _ in iter_range_bounds(date(2026, 1, 31), "month", count=4)
        ]
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
        policy = RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1),
                             interval="month", count=2)
        assert tuple(policy.iter_bounds()) == (
            (_dt(2026, 1, 1), _dt(2026, 2, 1)),
            (_dt(2026, 2, 1), _dt(2026, 3, 1)),
        )

    def test_quarterly_bounds(self):
        policy = RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1),
                             interval="quarter", count=4)
        bounds = tuple(policy.iter_bounds())
        assert bounds[0] == (_dt(2026, 1, 1), _dt(2026, 4, 1))
        assert bounds[-1] == (_dt(2026, 10, 1), _dt(2027, 1, 1))

    def test_explicit_bounds_win_over_interval(self):
        policy = RangePolicy(
            columns=("created_at",),
            bounds=((_dt(2026, 1, 1), _dt(2026, 1, 16)),
                    (_dt(2026, 1, 16), _dt(2026, 2, 1))),
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
        monthly = RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1),
                              interval="month")
        quarterly = RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1),
                                interval="quarter")
        yearly = RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1),
                             interval="year")
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
            "logs_p0", "logs_p1", "logs_p2", "logs_p3",
        ]
        leaves = [spec for spec in specs if spec.level == 2]
        assert len(leaves) == 42
        assert {spec.parent for spec in leaves} == {
            "logs_p0", "logs_p1", "logs_p2", "logs_p3",
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
            policy=RangePolicy(columns=("attempt_created_at",), start=_dt(2026, 9, 1),
                               interval="month", count=2),
        )
        specs = tuple(plan.iter_specs())
        assert [spec.name for spec in specs] == [
            "job_logs_2026_09",
            "job_logs_2026_10",
        ]
        assert all(spec.level == 1 and spec.is_leaf for spec in specs)

    def test_branch_name_suffix_composes_with_child_leaves(self):
        plan = PostgresPartitionPlan(
            parent="logs", method="HASH", columns=("bucket",), modulus=1,
            branches=(HashBranch(0, RangePolicy(columns=("created_at",),
                                                start=_dt(2026, 1, 1),
                                                interval="month", count=1),
                                 name="cold"),),
        )
        specs = tuple(plan.iter_specs())
        assert [spec.name for spec in specs] == ["logs_cold", "logs_cold_2026_01"]

    def test_name_pattern_is_the_only_naming_hook(self):
        plan = PostgresPartitionPlan(
            parent="logs",
            method="RANGE",
            columns=("created_at",),
            policy=RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1),
                               interval="month", count=1),
            name_pattern="{parent}__{suffix}",
        )
        assert tuple(plan.iter_specs())[0].name == "logs__2026_01"

    def test_branches_and_policy_are_mutually_exclusive(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            PostgresPartitionPlan(
                parent="logs", method="RANGE", columns=("created_at",),
                policy=RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1)),
                branches=(RangeBranch(_dt(2026, 1, 1), _dt(2027, 1, 1)),),
            )

    def test_flat_policy_must_match_parent_method(self):
        with pytest.raises(ValueError, match="policy must be a RangePolicy"):
            PostgresPartitionPlan(
                parent="logs", method="RANGE", columns=("created_at",),
                policy=HashPolicy(columns=("bucket",), modulus=2),
            )

    def test_heterogeneous_child_policies_are_allowed(self):
        plan = PostgresPartitionPlan(
            parent="logs", method="HASH", columns=("bucket",), modulus=2,
            branches=(
                HashBranch(0, RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1),
                                          interval="month", count=1)),
                HashBranch(1, ListPolicy(columns=("region",), groups=(("eu",),))),
            ),
        )
        assert [spec.method for spec in plan.iter_specs()][3] == "LIST"

    def test_duplicate_generated_names_are_rejected(self):
        plan = PostgresPartitionPlan(
            parent="logs", method="HASH", columns=("bucket",), modulus=4,
            branches=(HashBranch(0), HashBranch(0)),
        )
        with pytest.raises(ValueError, match="duplicate partition name"):
            tuple(plan.iter_specs())

    def test_hash_branch_requires_modulus(self):
        with pytest.raises(ValueError, match="requires modulus"):
            PostgresPartitionPlan(
                parent="logs", method="HASH", columns=("bucket",),
                branches=(HashBranch(0),),
            )

    def test_list_branch_only_for_list_parent(self):
        with pytest.raises(ValueError, match="RANGE parent's branches"):
            PostgresPartitionPlan(
                parent="logs", method="RANGE", columns=("created_at",),
                branches=(ListBranch(("eu",)),),
            )


class TestDdlRendering:
    def _maintenance(self):
        return PostgresPartitionMaintenance(_FakeBackend([]))

    def test_branch_render_appends_child_partition_clause(self):
        maintenance = self._maintenance()
        plan = _multilevel_plan()
        branch = next(plan.iter_specs())
        sql, params = maintenance._create_statement(plan, branch, if_not_exists=True)
        assert sql == (
            'CREATE TABLE IF NOT EXISTS "logs_p0" PARTITION OF "logs" '
            "FOR VALUES WITH (MODULUS 4, REMAINDER 0) "
            'PARTITION BY RANGE ("created_at")'
        )
        assert params == ()

    def test_leaf_render_is_range_of_utc_bounds(self):
        maintenance = self._maintenance()
        plan = _multilevel_plan()
        leaf = tuple(plan.iter_specs())[1]
        sql, params = maintenance._create_statement(plan, leaf, if_not_exists=False)
        assert sql == (
            'CREATE TABLE "logs_p0_2026_01" PARTITION OF "logs_p0" '
            "FOR VALUES FROM ('2026-01-01 00:00:00+00:00') "
            "TO ('2026-02-01 00:00:00+00:00')"
        )

    def test_drop_render_never_cascades_by_default(self):
        maintenance = self._maintenance()
        plan = _multilevel_plan()
        sql, _ = maintenance._drop_statement(plan, "logs_p0_2026_01", cascade=False)
        assert sql == 'DROP TABLE IF EXISTS "logs_p0_2026_01"'

    def test_tree_query_is_a_single_parameter_expression(self):
        maintenance = self._maintenance()
        plan = _multilevel_plan()
        sql, params = maintenance._tree_expression(plan).to_sql()
        assert "WITH RECURSIVE partition_tree" in sql
        assert "pg_inherits" in sql
        assert "pg_get_expr(cls.relpartbound" in sql
        assert sql.count("%s") == 1
        assert params == ("logs",)


class TestReportsWithoutServer:
    def test_list_finds_missing_and_orphans(self):
        plan = PostgresPartitionPlan(
            parent="logs", method="RANGE", columns=("created_at",),
            policy=RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1),
                               interval="month", count=3),
        )
        rows = [
            _catalog_row("logs", None, 0, "RANGE (created_at)", None, True),
            _catalog_row("logs_2026_01", "logs", 1,
                         bound="FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"),
            _catalog_row("logs_2025_12", "logs", 1,
                         bound="FOR VALUES FROM ('2025-12-01') TO ('2026-01-01')"),
        ]
        report = PostgresPartitionMaintenance(_FakeBackend(rows)).list(plan)
        assert report.missing == (("logs", "logs_2026_02"), ("logs", "logs_2026_03"))
        assert report.orphans == ("logs_2025_12",)
        assert report.issues == ()

    def test_unbounded_plan_does_not_call_extra_partitions_orphans(self):
        plan = PostgresPartitionPlan(
            parent="logs", method="RANGE", columns=("created_at",),
            policy=RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1)),
        )
        rows = [
            _catalog_row("logs", None, 0, "RANGE (created_at)", None, True),
            _catalog_row("logs_2026_01", "logs", 1,
                         bound="FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"),
        ]
        report = PostgresPartitionMaintenance(_FakeBackend(rows)).list(
            plan, through=_dt(2026, 2, 1)
        )
        assert report.orphans == ()
        assert report.missing == ()

    def test_validate_reports_gaps_and_overlaps(self):
        plan = PostgresPartitionPlan(
            parent="logs", method="RANGE", columns=("created_at",),
            policy=RangePolicy(
                columns=("created_at",),
                bounds=(
                    (_dt(2026, 1, 1), _dt(2026, 2, 1)),
                    (_dt(2026, 3, 1), _dt(2026, 4, 1)),
                    (_dt(2026, 3, 15), _dt(2026, 5, 1)),
                ),
            ),
        )
        rows = [_catalog_row("logs", None, 0, "RANGE (created_at)", None, True)]
        report = PostgresPartitionMaintenance(_FakeBackend(rows)).validate(plan)
        assert any(issue.startswith("gap in logs") for issue in report.issues)
        assert any(issue.startswith("overlap in logs") for issue in report.issues)

    def test_validate_reports_missing_parent(self):
        plan = PostgresPartitionPlan(
            parent="logs", method="RANGE", columns=("created_at",),
            policy=RangePolicy(columns=("created_at",), bounds=((_dt(2026, 1, 1),
                                                                 _dt(2026, 2, 1)),)),
        )
        report = PostgresPartitionMaintenance(_FakeBackend([])).validate(plan)
        assert report.partitions == ()
        assert report.issues == ("partitioned parent logs does not exist",)

    def test_ensure_creates_one_transaction_unit_per_branch(self):
        plan = PostgresPartitionPlan(
            parent="logs", method="HASH", columns=("bucket",), modulus=2,
            branches=(HashBranch(0), HashBranch(1)),
        )
        backend = _FakeBackend([_catalog_row("logs", None, 0, "HASH (bucket)", None, True)])
        maintenance = PostgresPartitionMaintenance(backend)
        report = maintenance.ensure(plan)
        assert backend.transaction_count == 2
        assert [op.name for op in report.operations] == ["logs_p0", "logs_p1"]
        assert all("IF NOT EXISTS" in op.sql for op in report.operations)

    def test_ensure_raises_when_parent_is_absent(self):
        plan = PostgresPartitionPlan(
            parent="logs", method="RANGE", columns=("created_at",),
            policy=RangePolicy(columns=("created_at",), bounds=((_dt(2026, 1, 1),
                                                                 _dt(2026, 2, 1)),)),
        )
        with pytest.raises(ValueError, match="does not exist"):
            PostgresPartitionMaintenance(_FakeBackend([])).ensure(plan)

    def test_drop_before_dry_run_plans_without_executing(self):
        plan = PostgresPartitionPlan(
            parent="logs", method="RANGE", columns=("created_at",),
            policy=RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1),
                               interval="month", count=3),
        )
        rows = [
            _catalog_row("logs", None, 0, "RANGE (created_at)", None, True),
            _catalog_row("logs_2026_01", "logs", 1,
                         bound="FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"),
            _catalog_row("logs_2026_02", "logs", 1,
                         bound="FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')"),
            _catalog_row("logs_2026_03", "logs", 1,
                         bound="FOR VALUES FROM ('2026-03-01') TO ('2026-04-01')"),
        ]
        backend = _FakeBackend(rows)
        maintenance = PostgresPartitionMaintenance(backend)
        report = maintenance.drop_before(plan, _dt(2026, 2, 1), dry_run=True)
        assert report.dry_run is True
        assert [op.name for op in report.operations] == ["logs_2026_01"]
        assert backend.executed == []

    def test_drop_before_never_drops_a_partitioned_branch(self):
        plan = _multilevel_plan()
        rows = [
            _catalog_row("logs", None, 0, "HASH (tenant_bucket)", None, True),
            _catalog_row("logs_p0", "logs", 1, "RANGE (created_at)",
                         "FOR VALUES WITH (MODULUS 4, REMAINDER 0)", True),
            _catalog_row("logs_p0_2026_01", "logs_p0", 2,
                         bound="FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"),
        ]
        backend = _FakeBackend(rows)
        report = PostgresPartitionMaintenance(backend).drop_before(
            plan, _dt(2026, 2, 1), dry_run=False
        )
        dropped = [op.name for op in report.operations]
        assert dropped == ["logs_p0_2026_01"]
        assert "logs_p0" not in dropped
        assert all('DROP TABLE IF EXISTS "' in sql for sql, _ in backend.executed)

    def test_drop_before_ignores_branches_that_start_at_or_after_cutoff(self):
        plan = PostgresPartitionPlan(
            parent="events", method="HASH", columns=("tenant_bucket",), modulus=4,
            branches=(
                HashBranch(0, RangePolicy(
                    columns=("created_at",), start=_dt(2026, 1, 1),
                    interval="month", count=12)),
                HashBranch(1, RangePolicy(
                    columns=("created_at",), start=_dt(2026, 1, 1),
                    interval="quarter", count=4)),
                HashBranch(2, RangePolicy(
                    columns=("created_at",), start=_dt(2026, 2, 1),
                    interval="day", count=28)),
                HashBranch(3),
            ),
        )
        rows = [
            _catalog_row("events", None, 0, "HASH (tenant_bucket)", None, True),
            _catalog_row("events_p0", "events", 1, "RANGE (created_at)",
                         "FOR VALUES WITH (MODULUS 4, REMAINDER 0)", True),
            _catalog_row("events_p1", "events", 1, "RANGE (created_at)",
                         "FOR VALUES WITH (MODULUS 4, REMAINDER 1)", True),
            _catalog_row("events_p2", "events", 1, "RANGE (created_at)",
                         "FOR VALUES WITH (MODULUS 4, REMAINDER 2)", True),
            _catalog_row("events_p3", "events", 1, None,
                         "FOR VALUES WITH (MODULUS 4, REMAINDER 3)", False),
            _catalog_row("events_p0_2026_01", "events_p0", 2,
                         bound="FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"),
        ]
        backend = _FakeBackend(rows)
        report = PostgresPartitionMaintenance(backend).drop_before(
            plan, _dt(2026, 2, 1), dry_run=True,
        )

        assert [op.name for op in report.operations] == ["events_p0_2026_01"]
        assert backend.executed == []

    def test_drop_branch_renders_cascade(self):
        plan = _multilevel_plan()
        maintenance = PostgresPartitionMaintenance(_FakeBackend([]))
        sql, _ = maintenance._drop_statement(plan, "logs_p0", cascade=True)
        assert sql == 'DROP TABLE IF EXISTS "logs_p0" CASCADE'


class TestBoundaryValidation:
    """The catalog's actual ``relpartbound`` text is compared with the plan."""

    @staticmethod
    def _plan(count=2):
        return PostgresPartitionPlan(
            parent="logs", method="RANGE", columns=("created_at",),
            policy=RangePolicy(
                columns=("created_at",), start=_dt(2026, 1, 1),
                interval="month", count=count,
            ),
        )

    @staticmethod
    def _rows():
        return [
            _catalog_row("logs", None, 0, "RANGE (created_at)", None, True),
            _catalog_row(
                "logs_2026_01", "logs", 1,
                bound="FOR VALUES FROM ('2025-12-01 00:00:00+00') "
                       "TO ('2026-01-01 00:00:00+00')",
            ),
            _catalog_row(
                "logs_2026_02", "logs", 1,
                bound="FOR VALUES FROM ('2026-02-01 00:00:00+00') "
                       "TO ('2026-03-01 00:00:00+00')",
            ),
        ]

    def test_parsed_range_bounds_are_retained_in_extras(self):
        text = (
            "FOR VALUES FROM ('2026-01-01 00:00:00+00') "
            "TO ('2026-02-01 00:00:00+00')"
        )
        rows = [_catalog_row("logs_2026_01", "logs", 1, bound=text)]
        infos = PostgresPartitionMaintenance._parse_rows(rows)

        assert infos[0].bound == text
        assert infos[0].extras["range_start"] == _dt(2026, 1, 1)
        assert infos[0].extras["range_end"] == _dt(2026, 2, 1)

    def test_range_boundary_mismatch_is_reported(self):
        report = PostgresPartitionMaintenance(
            _FakeBackend(self._rows())
        ).validate(self._plan())

        mismatches = [
            issue for issue in report.issues
            if issue.startswith("boundary mismatch for logs_2026_01")
        ]
        assert len(mismatches) == 1
        assert "catalog 2025-12-01 00:00:00+00:00..2026-01-01 00:00:00+00:00" in (
            mismatches[0]
        )
        assert "plan 2026-01-01 00:00:00+00:00..2026-02-01 00:00:00+00:00" in (
            mismatches[0]
        )

    def test_ensure_does_not_treat_a_mismatched_partition_as_present(self):
        plan = self._plan(count=3)
        backend = _FakeBackend(self._rows())
        report = PostgresPartitionMaintenance(backend).ensure(plan)

        assert {op.name for op in report.operations} == {
            "logs_2026_01", "logs_2026_03",
        }
        missing_names = {name for _, name in report.missing}
        assert missing_names == {"logs_2026_01", "logs_2026_03"}
        assert any(
            issue.startswith("boundary mismatch for logs_2026_01")
            for issue in report.issues
        )

    def test_list_boundary_mismatch_is_reported(self):
        plan = PostgresPartitionPlan(
            parent="events", method="LIST", columns=("region",),
            policy=ListPolicy(columns=("region",), groups=(("eu",), ("us",))),
        )
        rows = [
            _catalog_row("events", None, 0, "LIST (region)", None, True),
            _catalog_row(
                "events_eu", "events", 1,
                bound="FOR VALUES IN ('eu', 'ca')",
            ),
            _catalog_row("events_us", "events", 1, bound="FOR VALUES IN ('us')"),
        ]
        report = PostgresPartitionMaintenance(_FakeBackend(rows)).validate(plan)

        assert any(
            issue.startswith("boundary mismatch for events_eu")
            and "catalog values" in issue
            for issue in report.issues
        )
        assert not any("events_us" in issue for issue in report.issues)

    def test_hash_boundary_mismatch_is_reported(self):
        plan = PostgresPartitionPlan(
            parent="sessions", method="HASH", columns=("tenant_id",),
            policy=HashPolicy(
                columns=("tenant_id",), modulus=2, remainders=(0, 1),
            ),
        )
        rows = [
            _catalog_row("sessions", None, 0, "HASH (tenant_id)", None, True),
            _catalog_row(
                "sessions_p0", "sessions", 1,
                bound="FOR VALUES WITH (MODULUS 2, REMAINDER 0)",
            ),
            _catalog_row(
                "sessions_p1", "sessions", 1,
                bound="FOR VALUES WITH (MODULUS 4, REMAINDER 1)",
            ),
        ]
        report = PostgresPartitionMaintenance(_FakeBackend(rows)).validate(plan)

        assert any(
            issue.startswith("boundary mismatch for sessions_p1")
            and "modulus" in issue
            for issue in report.issues
        )
        assert not any("sessions_p0" in issue for issue in report.issues)

    def test_unparseable_bound_is_reported_not_silently_passed(self):
        plan = self._plan(count=1)
        rows = [
            _catalog_row("logs", None, 0, "RANGE (created_at)", None, True),
            _catalog_row(
                "logs_2026_01", "logs", 1,
                bound="FOR VALUES FROM (MINVALUE) "
                      "TO ('2026-02-01 00:00:00+00')",
            ),
        ]
        report = PostgresPartitionMaintenance(_FakeBackend(rows)).validate(plan)

        assert any(
            issue.startswith("unparsed bound for logs_2026_01")
            for issue in report.issues
        )


class TestUnboundedPlanReports:
    """An unbounded plan lists the tree instead of raising."""

    @staticmethod
    def _plan():
        return PostgresPartitionPlan(
            parent="logs", method="RANGE", columns=("created_at",),
            policy=RangePolicy(columns=("created_at",), start=_dt(2026, 1, 1)),
        )

    @staticmethod
    def _rows():
        return [
            _catalog_row("logs", None, 0, "RANGE (created_at)", None, True),
            _catalog_row(
                "logs_2026_01", "logs", 1,
                bound="FOR VALUES FROM ('2026-01-01 00:00:00+00') "
                      "TO ('2026-02-01 00:00:00+00')",
            ),
        ]

    def test_list_returns_the_catalog_tree_with_a_horizon_issue(self):
        plan = self._plan()
        report = PostgresPartitionMaintenance(_FakeBackend(self._rows())).list(plan)

        assert len(report.partitions) == 2
        assert report.missing == ()
        assert report.orphans == ()
        assert "plan has no finite horizon" in report.issues

    def test_validate_returns_the_same_partial_report(self):
        plan = self._plan()
        report = PostgresPartitionMaintenance(
            _FakeBackend(self._rows())
        ).validate(plan)

        assert [info.name for info in report.partitions] == [
            "logs", "logs_2026_01",
        ]
        assert report.missing == ()
        assert "plan has no finite horizon" in report.issues

    def test_ensure_rejects_unbounded_plan_but_drop_before_uses_the_cutoff(self):
        plan = self._plan()
        backend = _FakeBackend(self._rows())
        maintenance = PostgresPartitionMaintenance(backend)

        with pytest.raises(ValueError, match="plan has no finite horizon"):
            maintenance.ensure(plan)
        report = maintenance.drop_before(plan, _dt(2026, 2, 1), dry_run=True)
        assert [op.name for op in report.operations] == ["logs_2026_01"]
        assert backend.executed == []


class TestConcurrentDetach:
    """``drop_before`` can render detach-then-drop on PostgreSQL 14+."""

    @staticmethod
    def _plan(count=2):
        return PostgresPartitionPlan(
            parent="logs", method="RANGE", columns=("created_at",),
            policy=RangePolicy(
                columns=("created_at",), start=_dt(2026, 1, 1),
                interval="month", count=count,
            ),
        )

    @staticmethod
    def _rows():
        return [
            _catalog_row("logs", None, 0, "RANGE (created_at)", None, True),
            _catalog_row(
                "logs_2026_01", "logs", 1,
                bound="FOR VALUES FROM ('2026-01-01 00:00:00+00') "
                      "TO ('2026-02-01 00:00:00+00')",
            ),
            _catalog_row(
                "logs_2026_02", "logs", 1,
                bound="FOR VALUES FROM ('2026-02-01 00:00:00+00') "
                      "TO ('2026-03-01 00:00:00+00')",
            ),
        ]

    def test_dry_run_renders_detach_then_drop_without_executing(self):
        backend = _FakeBackend(self._rows())
        report = PostgresPartitionMaintenance(backend).drop_before(
            self._plan(), _dt(2026, 2, 1), dry_run=True,
            concurrent_detach=True,
        )

        assert report.dry_run is True
        assert [(op.action, op.name) for op in report.operations] == [
            ("detach", "logs_2026_01"), ("drop", "logs_2026_01"),
        ]
        assert report.operations[0].sql == (
            'ALTER TABLE "logs" DETACH PARTITION "logs_2026_01" CONCURRENTLY'
        )
        assert report.operations[1].sql == 'DROP TABLE IF EXISTS "logs_2026_01"'
        assert backend.executed == []

    def test_pg14_executes_detach_then_drop_without_a_transaction(self):
        backend = _FakeBackend(self._rows(), version=(17, 0, 0))
        PostgresPartitionMaintenance(backend).drop_before(
            self._plan(), _dt(2026, 2, 1), dry_run=False,
            concurrent_detach=True,
        )

        assert backend.executed == [
            (
                'ALTER TABLE "logs" DETACH PARTITION "logs_2026_01" CONCURRENTLY',
                (),
            ),
            ('DROP TABLE IF EXISTS "logs_2026_01"', ()),
        ]
        assert backend.transaction_count == 0

    def test_pg13_falls_back_to_the_plain_drop(self):
        backend = _FakeBackend(self._rows(), version=(13, 0, 0))
        assert backend.dialect.supports_concurrent_detach() is False
        report = PostgresPartitionMaintenance(backend).drop_before(
            self._plan(), _dt(2026, 2, 1), dry_run=True,
            concurrent_detach=True,
        )

        assert [(op.action, op.name) for op in report.operations] == [
            ("drop", "logs_2026_01"),
        ]
        assert "DETACH PARTITION" not in report.operations[0].sql

    def test_pg13_fallback_still_uses_one_transaction_per_unit(self):
        backend = _FakeBackend(self._rows(), version=(13, 0, 0))
        PostgresPartitionMaintenance(backend).drop_before(
            self._plan(), _dt(2026, 2, 1), dry_run=False,
            concurrent_detach=True,
        )

        assert backend.executed == [('DROP TABLE IF EXISTS "logs_2026_01"', ())]
        assert backend.transaction_count == 1

    def test_concurrent_detach_false_keeps_the_plain_drop_on_pg17(self):
        backend = _FakeBackend(self._rows(), version=(17, 0, 0))
        report = PostgresPartitionMaintenance(backend).drop_before(
            self._plan(), _dt(2026, 2, 1), dry_run=True,
        )

        assert [op.action for op in report.operations] == ["drop"]


class TestAsyncBoundaryAndHorizon:
    """Async parity for boundary validation, unbounded list and detach."""

    @pytest.mark.asyncio
    async def test_boundary_mismatch_is_reported(self):
        plan = TestBoundaryValidation._plan()
        backend = _FakeAsyncBackend(TestBoundaryValidation._rows())
        report = await AsyncPostgresPartitionMaintenance(backend).validate(plan)

        assert any(
            issue.startswith("boundary mismatch for logs_2026_01")
            for issue in report.issues
        )

    @pytest.mark.asyncio
    async def test_unbounded_list_returns_the_catalog_tree(self):
        plan = TestUnboundedPlanReports._plan()
        backend = _FakeAsyncBackend(TestUnboundedPlanReports._rows())
        report = await AsyncPostgresPartitionMaintenance(backend).list(plan)

        assert len(report.partitions) == 2
        assert report.missing == ()
        assert "plan has no finite horizon" in report.issues

    @pytest.mark.asyncio
    async def test_unbounded_ensure_rejects_but_drop_before_uses_the_cutoff(self):
        plan = TestUnboundedPlanReports._plan()
        backend = _FakeAsyncBackend(TestUnboundedPlanReports._rows())
        maintenance = AsyncPostgresPartitionMaintenance(backend)

        with pytest.raises(ValueError, match="plan has no finite horizon"):
            await maintenance.ensure(plan)
        report = await maintenance.drop_before(plan, _dt(2026, 2, 1), dry_run=True)
        assert [op.name for op in report.operations] == ["logs_2026_01"]

    @pytest.mark.asyncio
    async def test_concurrent_detach_dry_run_renders_both_statements(self):
        plan = TestConcurrentDetach._plan()
        backend = _FakeAsyncBackend(TestConcurrentDetach._rows())
        report = await AsyncPostgresPartitionMaintenance(backend).drop_before(
            plan, _dt(2026, 2, 1), dry_run=True, concurrent_detach=True,
        )

        assert [(op.action, op.name) for op in report.operations] == [
            ("detach", "logs_2026_01"), ("drop", "logs_2026_01"),
        ]
        assert backend.executed == []

    @pytest.mark.asyncio
    async def test_async_pg13_falls_back_to_the_plain_drop(self):
        plan = TestConcurrentDetach._plan()
        backend = _FakeAsyncBackend(TestConcurrentDetach._rows(), version=(13, 0, 0))
        report = await AsyncPostgresPartitionMaintenance(backend).drop_before(
            plan, _dt(2026, 2, 1), dry_run=True, concurrent_detach=True,
        )

        assert [op.action for op in report.operations] == ["drop"]


class TestPortableMaintenanceContract:
    """The PostgreSQL executor speaks the core contract's shape and verbs."""

    def test_reports_are_the_core_value_objects(self):
        assert PartitionInfo is CorePartitionInfo
        assert PartitionReport is CorePartitionReport

    def test_catalog_bytes_map_to_the_core_size_field(self):
        rows = [_catalog_row("logs", None, 0, total_bytes=8192)]
        infos = PostgresPartitionMaintenance._parse_rows(rows)

        assert infos[0].size_bytes == 8192

    def test_the_sync_executor_offers_every_canonical_verb(self):
        executor = PostgresPartitionMaintenance(_FakeBackend([]))

        assert isinstance(executor, PartitionInventory)
        assert isinstance(executor, PartitionEnsurer)
        assert isinstance(executor, PartitionRetention)
        assert isinstance(executor, PartitionValidator)

    def test_the_async_executor_offers_every_canonical_verb(self):
        executor = AsyncPostgresPartitionMaintenance(_FakeBackend([]))

        assert isinstance(executor, AsyncPartitionInventory)
        assert isinstance(executor, AsyncPartitionEnsurer)
        assert isinstance(executor, AsyncPartitionRetention)
        assert isinstance(executor, AsyncPartitionValidator)


def _batch_plan(parent, count=2, interval="month"):
    return PostgresPartitionPlan(
        parent=parent,
        method="RANGE",
        columns=("created_at",),
        policy=RangePolicy(
            columns=("created_at",),
            start=_dt(2026, 1, 1),
            interval=interval,
            count=count,
        ),
    )


def _range_bound(start, end):
    return "FOR VALUES FROM ('%s') TO ('%s')" % (
        start.isoformat(),
        end.isoformat(),
    )


def _root_row(parent):
    return _catalog_row(parent, None, 0, "RANGE (created_at)", None, True)


def _planned_row(parent, label, start, end):
    return _catalog_row(
        "%s_%s" % (parent, label),
        parent,
        1,
        "RANGE (created_at)",
        _range_bound(start, end),
    )


def _rows_for_root(rows, root):
    """Return the rows belonging to *root* plus their descendants."""
    names = {root}
    changed = True
    while changed:
        changed = False
        for row in rows:
            if row["parent"] in names and row["name"] not in names:
                names.add(row["name"])
                changed = True
    return [row for row in rows if row["name"] in names]


def _apply_catalog_change(backend, sql):
    """Apply a rendered CREATE/DROP to the scripted catalogue."""
    if sql.startswith("CREATE TABLE") and "PARTITION OF" in sql:
        name = sql.split('"')[1]
        parent = sql.split("PARTITION OF ")[1].split('"')[1]
        bound = None
        if "FOR VALUES" in sql:
            bound = sql[sql.index("FOR VALUES"):]
            if " PARTITION BY " in bound:
                bound = bound.split(" PARTITION BY ")[0].strip()
        partition_key = None
        if bound and bound.startswith("FOR VALUES FROM"):
            partition_key = "RANGE (created_at)"
        elif bound and bound.startswith("FOR VALUES IN"):
            partition_key = "LIST (region)"
        elif bound and bound.startswith("FOR VALUES WITH"):
            partition_key = "HASH (bucket)"
        backend.rows.append(
            _catalog_row(
                name,
                parent,
                1,
                partition_key,
                bound,
                "PARTITION BY" in sql,
            )
        )
        return
    if sql.startswith("DROP TABLE"):
        name = sql.split('"')[1]
        backend.rows = [row for row in backend.rows if row["name"] != name]


class _SnapshotTransaction:
    def __init__(self, backend):
        self.backend = backend

    def __enter__(self):
        self.snapshot = list(self.backend.rows)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self.backend.rows = self.snapshot
        return False


class _AsyncSnapshotTransaction:
    def __init__(self, backend):
        self.backend = backend

    async def __aenter__(self):
        self.snapshot = list(self.backend.rows)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self.backend.rows = self.snapshot
        return False


class _MutableCatalogBackend(_FakeBackend):
    """Scripted backend that applies rendered DDL to its catalogue."""

    def __init__(self, rows, version=(17, 0, 0), fail_on=None):
        super().__init__(rows, version)
        self.fail_on = fail_on

    def fetch_all(self, sql, params=None):
        root = params[0] if params else None
        return _rows_for_root(self.rows, root)

    def execute(self, sql, params=None):
        if self.fail_on is not None and self.fail_on in sql:
            raise RuntimeError("scripted failure: %s" % self.fail_on)
        super().execute(sql, params)
        _apply_catalog_change(self, sql)

    def transaction(self):
        self.transaction_count += 1
        return _SnapshotTransaction(self)


class _MutableCatalogAsyncBackend(_FakeAsyncBackend):
    """Async scripted backend that applies rendered DDL to its catalogue."""

    def __init__(self, rows, version=(17, 0, 0), fail_on=None):
        super().__init__(rows, version)
        self.fail_on = fail_on

    async def fetch_all(self, sql, params=None):
        root = params[0] if params else None
        return _rows_for_root(self.rows, root)

    async def execute(self, sql, params=None):
        if self.fail_on is not None and self.fail_on in sql:
            raise RuntimeError("scripted failure: %s" % self.fail_on)
        self.executed.append((sql, tuple(params or ())))
        _apply_catalog_change(self, sql)

    def transaction(self):
        self.transaction_count += 1
        return _AsyncSnapshotTransaction(self)


class TestPartitionBatchEnsure:
    def test_independent_batch_creates_every_plan_in_one_transaction(self):
        plan_a = _batch_plan("job_logs", count=2)
        plan_b = _batch_plan("step_logs", count=2)
        backend = _MutableCatalogBackend([_root_row("job_logs"), _root_row("step_logs")])

        batch = PostgresPartitionMaintenance(backend).ensure_many(
            plans=[plan_a, plan_b]
        )

        assert backend.transaction_count == 1
        assert set(batch.reports) == {"job_logs", "step_logs"}
        assert batch.pairs == ()
        assert batch.differences == ()
        assert all(report.missing == () for report in batch.reports.values())
        assert len(backend.executed) == 4

    def test_a_later_plan_failure_rolls_back_the_whole_batch(self):
        plan_a = _batch_plan("job_logs", count=2)
        plan_b = _batch_plan("step_logs", count=2)
        backend = _MutableCatalogBackend(
            [_root_row("job_logs"), _root_row("step_logs")],
            fail_on="step_logs_2026_01",
        )

        with pytest.raises(RuntimeError, match="scripted failure"):
            PostgresPartitionMaintenance(backend).ensure_many(
                plans=[plan_a, plan_b]
            )

        assert backend.transaction_count == 1
        assert [row["name"] for row in backend.rows] == ["job_logs", "step_logs"]
        assert any('"job_logs_2026_01"' in sql for sql, _ in backend.executed)
        assert all(
            '"step_logs_2026_01"' not in sql for sql, _ in backend.executed
        )

    def test_paired_ensure_fills_both_sides_and_leaves_no_drift(self):
        plan_a = _batch_plan("job_logs", count=2)
        plan_b = _batch_plan("step_logs", count=2)
        backend = _MutableCatalogBackend([_root_row("job_logs"), _root_row("step_logs")])

        batch = PostgresPartitionMaintenance(backend).ensure_many(
            pairs=[("log_tables", plan_a, plan_b)]
        )

        assert batch.pairs == ("log_tables",)
        assert batch.differences == ()
        assert batch.issues == ()
        assert all(report.missing == () for report in batch.reports.values())


class TestPartitionBatchList:
    def test_paired_list_reports_left_only_and_right_only_buckets(self):
        left_plan = _batch_plan("job_logs", count=3)
        right_plan = _batch_plan("step_logs", count=3)
        rows = [
            _root_row("job_logs"),
            _planned_row("job_logs", "2026_01", _dt(2026, 1, 1), _dt(2026, 2, 1)),
            _planned_row("job_logs", "2026_02", _dt(2026, 2, 1), _dt(2026, 3, 1)),
            _root_row("step_logs"),
            _planned_row("step_logs", "2026_02", _dt(2026, 2, 1), _dt(2026, 3, 1)),
            _planned_row("step_logs", "2026_03", _dt(2026, 3, 1), _dt(2026, 4, 1)),
        ]
        batch = PostgresPartitionMaintenance(_MutableCatalogBackend(rows)).list_many(
            pairs=[("log_tables", left_plan, right_plan)]
        )

        assert [
            (diff.present_in, diff.left_partition, diff.right_partition)
            for diff in batch.differences
        ] == [
            ("left", "job_logs_2026_01", None),
            ("right", None, "step_logs_2026_03"),
        ]
        assert batch.issues == ()

    def test_two_side_absence_stays_in_each_report_missing(self):
        left_plan = _batch_plan("job_logs", count=3)
        right_plan = _batch_plan("step_logs", count=3)
        rows = [
            _root_row("job_logs"),
            _planned_row("job_logs", "2026_01", _dt(2026, 1, 1), _dt(2026, 2, 1)),
            _root_row("step_logs"),
            _planned_row("step_logs", "2026_01", _dt(2026, 1, 1), _dt(2026, 2, 1)),
        ]
        batch = PostgresPartitionMaintenance(_MutableCatalogBackend(rows)).list_many(
            pairs=[("log_tables", left_plan, right_plan)]
        )

        assert batch.differences == ()
        assert {name for _, name in batch.reports["job_logs"].missing} == {
            "job_logs_2026_02", "job_logs_2026_03",
        }
        assert {name for _, name in batch.reports["step_logs"].missing} == {
            "step_logs_2026_02", "step_logs_2026_03",
        }

    def test_boundary_text_disagreement_is_an_issue_not_a_difference(self):
        left_plan = _batch_plan("job_logs", count=1)
        right_plan = _batch_plan("step_logs", count=1)
        rows = [
            _root_row("job_logs"),
            _catalog_row(
                "job_logs_2026_01", "job_logs", 1, "RANGE (created_at)",
                "FOR VALUES FROM ('2026-01-01 00:00:00+00') "
                "TO ('2026-02-01 00:00:00+00')",
            ),
            _root_row("step_logs"),
            _catalog_row(
                "step_logs_2026_01", "step_logs", 1, "RANGE (created_at)",
                "FOR VALUES FROM ('2026-01-01 00:00:00+00:00') "
                "TO ('2026-02-01 00:00:00+00:00')",
            ),
        ]
        batch = PostgresPartitionMaintenance(_MutableCatalogBackend(rows)).list_many(
            pairs=[("log_tables", left_plan, right_plan)]
        )

        assert batch.differences == ()
        assert len(batch.issues) == 1
        assert batch.issues[0].startswith("boundary mismatch for log_tables/RANGE|")

    def test_incompatible_pair_is_an_issue_with_no_differences(self):
        left_plan = _batch_plan("job_logs", count=2, interval="month")
        right_plan = _batch_plan("step_logs", count=2, interval="quarter")
        rows = [_root_row("job_logs"), _root_row("step_logs")]
        batch = PostgresPartitionMaintenance(_MutableCatalogBackend(rows)).list_many(
            pairs=[("log_tables", left_plan, right_plan)]
        )

        assert batch.differences == ()
        assert batch.issues == ("pair log_tables plans are not comparable",)


class TestPartitionBatchDrop:
    def test_dry_run_reports_before_state_and_executes_nothing(self):
        plan = _batch_plan("logs", count=3)
        backend = _MutableCatalogBackend([
            _root_row("logs"),
            _planned_row("logs", "2026_01", _dt(2026, 1, 1), _dt(2026, 2, 1)),
        ])

        batch = PostgresPartitionMaintenance(backend).drop_before_many(
            plans=[plan],
            cutoff=_dt(2026, 2, 1),
            dry_run=True,
            through=_dt(2026, 4, 1),
        )

        assert backend.executed == []
        assert backend.transaction_count == 0
        report = batch.reports["logs"]
        assert report.dry_run is True
        assert [(op.action, op.name) for op in report.operations] == [
            ("drop", "logs_2026_01"),
        ]
        assert {name for _, name in report.missing} == {
            "logs_2026_02", "logs_2026_03",
        }
        assert any(info.name == "logs_2026_01" for info in report.partitions)

    def test_real_run_uses_one_transaction_and_reports_post_state(self):
        plan = _batch_plan("logs", count=3)
        backend = _MutableCatalogBackend([
            _root_row("logs"),
            _planned_row("logs", "2026_01", _dt(2026, 1, 1), _dt(2026, 2, 1)),
        ])

        batch = PostgresPartitionMaintenance(backend).drop_before_many(
            plans=[plan],
            cutoff=_dt(2026, 2, 1),
            dry_run=False,
            through=_dt(2026, 4, 1),
        )

        assert backend.transaction_count == 1
        assert [op.name for op in batch.reports["logs"].operations] == [
            "logs_2026_01",
        ]
        assert "logs_2026_01" not in {
            info.name for info in batch.reports["logs"].partitions
        }
        assert {name for _, name in batch.reports["logs"].missing} == {
            "logs_2026_02", "logs_2026_03",
        }

    def test_concurrent_detach_with_several_plans_keeps_one_transaction(self):
        plan_a = _batch_plan("job_logs", count=2)
        plan_b = _batch_plan("step_logs", count=2)
        backend = _MutableCatalogBackend([_root_row("job_logs"), _root_row("step_logs")])

        batch = PostgresPartitionMaintenance(backend).drop_before_many(
            plans=[plan_a, plan_b],
            cutoff=_dt(2026, 2, 1),
            dry_run=False,
            concurrent_detach=True,
        )

        assert any(
            "concurrent detach requires a single plan" in issue
            for issue in batch.issues
        )
        assert backend.transaction_count == 1
        assert all(
            op.action == "drop"
            for report in batch.reports.values()
            for op in report.operations
        )

    def test_concurrent_detach_with_one_plan_skips_the_transaction(self):
        plan = _batch_plan("logs", count=3)
        backend = _MutableCatalogBackend([
            _root_row("logs"),
            _planned_row("logs", "2026_01", _dt(2026, 1, 1), _dt(2026, 2, 1)),
        ])

        batch = PostgresPartitionMaintenance(backend).drop_before_many(
            plans=[plan],
            cutoff=_dt(2026, 2, 1),
            dry_run=False,
            concurrent_detach=True,
        )

        assert backend.transaction_count == 0
        assert [(op.action, op.name) for op in batch.reports["logs"].operations] == [
            ("detach", "logs_2026_01"),
            ("drop", "logs_2026_01"),
        ]


class TestAsyncPartitionBatchEnsure:
    @pytest.mark.asyncio
    async def test_independent_batch_creates_every_plan_in_one_transaction(self):
        plan_a = _batch_plan("job_logs", count=2)
        plan_b = _batch_plan("step_logs", count=2)
        backend = _MutableCatalogAsyncBackend([
            _root_row("job_logs"), _root_row("step_logs"),
        ])

        batch = await AsyncPostgresPartitionMaintenance(backend).ensure_many(
            plans=[plan_a, plan_b]
        )

        assert backend.transaction_count == 1
        assert set(batch.reports) == {"job_logs", "step_logs"}
        assert batch.differences == ()
        assert all(report.missing == () for report in batch.reports.values())
        assert len(backend.executed) == 4

    @pytest.mark.asyncio
    async def test_a_later_plan_failure_rolls_back_the_whole_batch(self):
        plan_a = _batch_plan("job_logs", count=2)
        plan_b = _batch_plan("step_logs", count=2)
        backend = _MutableCatalogAsyncBackend(
            [_root_row("job_logs"), _root_row("step_logs")],
            fail_on="step_logs_2026_01",
        )

        with pytest.raises(RuntimeError, match="scripted failure"):
            await AsyncPostgresPartitionMaintenance(backend).ensure_many(
                plans=[plan_a, plan_b]
            )

        assert backend.transaction_count == 1
        assert [row["name"] for row in backend.rows] == ["job_logs", "step_logs"]

    @pytest.mark.asyncio
    async def test_paired_ensure_fills_both_sides_and_leaves_no_drift(self):
        plan_a = _batch_plan("job_logs", count=2)
        plan_b = _batch_plan("step_logs", count=2)
        backend = _MutableCatalogAsyncBackend([
            _root_row("job_logs"), _root_row("step_logs"),
        ])

        batch = await AsyncPostgresPartitionMaintenance(backend).ensure_many(
            pairs=[("log_tables", plan_a, plan_b)]
        )

        assert batch.pairs == ("log_tables",)
        assert batch.differences == ()
        assert all(report.missing == () for report in batch.reports.values())


class TestAsyncPartitionBatchList:
    @pytest.mark.asyncio
    async def test_paired_list_reports_left_only_and_right_only_buckets(self):
        left_plan = _batch_plan("job_logs", count=3)
        right_plan = _batch_plan("step_logs", count=3)
        rows = [
            _root_row("job_logs"),
            _planned_row("job_logs", "2026_01", _dt(2026, 1, 1), _dt(2026, 2, 1)),
            _planned_row("job_logs", "2026_02", _dt(2026, 2, 1), _dt(2026, 3, 1)),
            _root_row("step_logs"),
            _planned_row("step_logs", "2026_02", _dt(2026, 2, 1), _dt(2026, 3, 1)),
            _planned_row("step_logs", "2026_03", _dt(2026, 3, 1), _dt(2026, 4, 1)),
        ]
        batch = await AsyncPostgresPartitionMaintenance(
            _MutableCatalogAsyncBackend(rows)
        ).list_many(pairs=[("log_tables", left_plan, right_plan)])

        assert [
            (diff.present_in, diff.left_partition, diff.right_partition)
            for diff in batch.differences
        ] == [
            ("left", "job_logs_2026_01", None),
            ("right", None, "step_logs_2026_03"),
        ]

    @pytest.mark.asyncio
    async def test_two_side_absence_stays_in_each_report_missing(self):
        left_plan = _batch_plan("job_logs", count=3)
        right_plan = _batch_plan("step_logs", count=3)
        rows = [
            _root_row("job_logs"),
            _planned_row("job_logs", "2026_01", _dt(2026, 1, 1), _dt(2026, 2, 1)),
            _root_row("step_logs"),
            _planned_row("step_logs", "2026_01", _dt(2026, 1, 1), _dt(2026, 2, 1)),
        ]
        batch = await AsyncPostgresPartitionMaintenance(
            _MutableCatalogAsyncBackend(rows)
        ).list_many(pairs=[("log_tables", left_plan, right_plan)])

        assert batch.differences == ()
        assert {name for _, name in batch.reports["job_logs"].missing} == {
            "job_logs_2026_02", "job_logs_2026_03",
        }

    @pytest.mark.asyncio
    async def test_boundary_text_disagreement_is_an_issue_not_a_difference(self):
        left_plan = _batch_plan("job_logs", count=1)
        right_plan = _batch_plan("step_logs", count=1)
        rows = [
            _root_row("job_logs"),
            _catalog_row(
                "job_logs_2026_01", "job_logs", 1, "RANGE (created_at)",
                "FOR VALUES FROM ('2026-01-01 00:00:00+00') "
                "TO ('2026-02-01 00:00:00+00')",
            ),
            _root_row("step_logs"),
            _catalog_row(
                "step_logs_2026_01", "step_logs", 1, "RANGE (created_at)",
                "FOR VALUES FROM ('2026-01-01 00:00:00+00:00') "
                "TO ('2026-02-01 00:00:00+00:00')",
            ),
        ]
        batch = await AsyncPostgresPartitionMaintenance(
            _MutableCatalogAsyncBackend(rows)
        ).list_many(pairs=[("log_tables", left_plan, right_plan)])

        assert batch.differences == ()
        assert len(batch.issues) == 1
        assert batch.issues[0].startswith("boundary mismatch for log_tables/RANGE|")

    @pytest.mark.asyncio
    async def test_incompatible_pair_is_an_issue_with_no_differences(self):
        left_plan = _batch_plan("job_logs", count=2, interval="month")
        right_plan = _batch_plan("step_logs", count=2, interval="quarter")
        rows = [_root_row("job_logs"), _root_row("step_logs")]
        batch = await AsyncPostgresPartitionMaintenance(
            _MutableCatalogAsyncBackend(rows)
        ).list_many(pairs=[("log_tables", left_plan, right_plan)])

        assert batch.differences == ()
        assert batch.issues == ("pair log_tables plans are not comparable",)


class TestAsyncPartitionBatchDrop:
    @pytest.mark.asyncio
    async def test_dry_run_reports_before_state_and_executes_nothing(self):
        plan = _batch_plan("logs", count=3)
        backend = _MutableCatalogAsyncBackend([
            _root_row("logs"),
            _planned_row("logs", "2026_01", _dt(2026, 1, 1), _dt(2026, 2, 1)),
        ])

        batch = await AsyncPostgresPartitionMaintenance(backend).drop_before_many(
            plans=[plan],
            cutoff=_dt(2026, 2, 1),
            dry_run=True,
            through=_dt(2026, 4, 1),
        )

        assert backend.executed == []
        assert backend.transaction_count == 0
        report = batch.reports["logs"]
        assert report.dry_run is True
        assert [(op.action, op.name) for op in report.operations] == [
            ("drop", "logs_2026_01"),
        ]
        assert any(info.name == "logs_2026_01" for info in report.partitions)

    @pytest.mark.asyncio
    async def test_real_run_uses_one_transaction_and_reports_post_state(self):
        plan = _batch_plan("logs", count=3)
        backend = _MutableCatalogAsyncBackend([
            _root_row("logs"),
            _planned_row("logs", "2026_01", _dt(2026, 1, 1), _dt(2026, 2, 1)),
        ])

        batch = await AsyncPostgresPartitionMaintenance(backend).drop_before_many(
            plans=[plan],
            cutoff=_dt(2026, 2, 1),
            dry_run=False,
            through=_dt(2026, 4, 1),
        )

        assert backend.transaction_count == 1
        assert [op.name for op in batch.reports["logs"].operations] == [
            "logs_2026_01",
        ]
        assert "logs_2026_01" not in {
            info.name for info in batch.reports["logs"].partitions
        }

    @pytest.mark.asyncio
    async def test_concurrent_detach_with_several_plans_keeps_one_transaction(self):
        plan_a = _batch_plan("job_logs", count=2)
        plan_b = _batch_plan("step_logs", count=2)
        backend = _MutableCatalogAsyncBackend([
            _root_row("job_logs"), _root_row("step_logs"),
        ])

        batch = await AsyncPostgresPartitionMaintenance(backend).drop_before_many(
            plans=[plan_a, plan_b],
            cutoff=_dt(2026, 2, 1),
            dry_run=False,
            concurrent_detach=True,
        )

        assert any(
            "concurrent detach requires a single plan" in issue
            for issue in batch.issues
        )
        assert backend.transaction_count == 1
        assert all(
            op.action == "drop"
            for report in batch.reports.values()
            for op in report.operations
        )
