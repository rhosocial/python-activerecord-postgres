# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/partition_syntax_fixtures.py
"""Fixture table for PostgreSQL partition syntax families.

Every case is an offline description of one PostgreSQL partitioning shape:
the plan (or a statement-only clause case), the exact specs the plan must
generate, the DDL fragments that must and must not appear, and -- where a
family has range leaves -- the ``drop_before`` selection the executor must
make.

The matrix deliberately lives in fixtures rather than in test bodies: adding
a new PostgreSQL syntax family is one entry in :data:`CASES`, and the tests in
``test_partition_syntax_matrix.py`` apply the same checks to it.

DEFAULT partitions are not represented here. The statement layer can render
``PARTITION OF ... DEFAULT`` (``partition_values={"default": True}``), but
``RangePolicy``/``ListPolicy``/``HashPolicy`` and therefore
``PostgresPartitionPlan`` have no way to describe an unbounded DEFAULT child,
so a plan-driven DEFAULT case would not exercise the plan API at all. It is
reported as unsupported rather than encoded as a misleading case.
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional, Tuple

import pytest

from rhosocial.activerecord.backend.impl.postgres.expression.ddl.partition import (
    PostgresAttachPartitionStatement,
    PostgresDetachPartitionStatement,
)
from rhosocial.activerecord.backend.impl.postgres.partition import (
    HashBranch,
    HashPolicy,
    ListBranch,
    ListPolicy,
    PostgresPartitionPlan,
    RangeBranch,
    RangePolicy,
)

UTC = timezone.utc


def _dt(year: int, month: int, day: int) -> datetime:
    """Build a timezone-aware UTC datetime used by every generated bound."""
    return datetime(year, month, day, tzinfo=UTC)


@dataclass(frozen=True)
class ExpectedSpecFacts:
    """One expected ``PartitionSpec``, written as data instead of a guess."""

    name: str
    parent: str
    level: int
    is_leaf: bool
    method: str
    bounds: Dict[str, Any]
    child_method: Optional[str] = None
    child_columns: Tuple[str, ...] = ()


@dataclass(frozen=True)
class ClauseStatement:
    """One statement-only case: how to build it and the SQL it must render."""

    kind: str
    build: Callable[[Any], Any]
    expected_sql: str


@dataclass(frozen=True)
class ClauseCase:
    """Statement cases that have no partition plan (ATTACH/DETACH)."""

    statements: Tuple[ClauseStatement, ...]
    concurrent_attach_rejected: bool = False


@dataclass(frozen=True)
class DropBeforeExpectation:
    """The cutoff and the leaf names ``drop_before`` must select."""

    cutoff: Any
    candidates: Tuple[str, ...]


@dataclass(frozen=True)
class PartitionSyntaxCase:
    """One fixtures-driven PostgreSQL partition syntax family.

    Attributes:
        name: Stable pytest id.
        description: What syntax family this case pins.
        plan: Plan-driven case, or ``None`` for a statement-only clause case.
        clause: Statement-only case, or ``None`` for a plan-driven case.
        expected_specs: Exact specs the plan must generate, in yielded order.
        required_fragments: Substrings every rendered DDL set must contain.
        forbidden_fragments: Substrings no rendered DDL statement may contain.
        drop_before: Optional retention-selection expectation.
        branch_leaf_counts: ``(branch name, direct child leaf count)`` pairs
            for multi-level cases; the branch shape is checked as well.
        live_setup: Optional callable or SQL text for a future live test.
        live_check: Optional callable or SQL text for a future live test.
    """

    name: str
    description: str
    plan: Optional[PostgresPartitionPlan] = None
    clause: Optional[ClauseCase] = None
    expected_specs: Tuple[ExpectedSpecFacts, ...] = ()
    required_fragments: Tuple[str, ...] = ()
    forbidden_fragments: Tuple[str, ...] = ()
    drop_before: Optional[DropBeforeExpectation] = None
    branch_leaf_counts: Tuple[Tuple[str, int], ...] = ()
    live_setup: Optional[Any] = None
    live_check: Optional[Any] = None

    def __post_init__(self) -> None:
        if (self.plan is None) == (self.clause is None):
            raise ValueError("exactly one of plan or clause must be supplied")


def _leaf(name: str, parent: str, method: str, bounds: Dict[str, Any],
          level: int = 1) -> ExpectedSpecFacts:
    return ExpectedSpecFacts(
        name=name, parent=parent, level=level, is_leaf=True,
        method=method, bounds=bounds,
    )


def _branch(name: str, parent: str, method: str, bounds: Dict[str, Any],
            child_method: Optional[str] = None,
            child_columns: Tuple[str, ...] = ()) -> ExpectedSpecFacts:
    return ExpectedSpecFacts(
        name=name, parent=parent, level=1, is_leaf=child_method is None,
        method=method, bounds=bounds, child_method=child_method,
        child_columns=child_columns,
    )


def _monthly_leaf_facts(parent: str, year: int, start_month: int,
                        count: int, level: int = 2) -> Tuple[ExpectedSpecFacts, ...]:
    facts = []
    for offset in range(count):
        index = (start_month - 1) + offset
        current_year = year + index // 12
        current_month = index % 12 + 1
        start = _dt(current_year, current_month, 1)
        if current_month == 12:
            end = _dt(current_year + 1, 1, 1)
        else:
            end = _dt(current_year, current_month + 1, 1)
        name = "%s_%d_%02d" % (parent, current_year, current_month)
        facts.append(_leaf(name, parent, "RANGE", {"from": start, "to": end},
                           level=level))
    return tuple(facts)


def _quarterly_leaf_facts(parent: str) -> Tuple[ExpectedSpecFacts, ...]:
    pairs = (
        (_dt(2026, 1, 1), _dt(2026, 4, 1)),
        (_dt(2026, 4, 1), _dt(2026, 7, 1)),
        (_dt(2026, 7, 1), _dt(2026, 10, 1)),
        (_dt(2026, 10, 1), _dt(2027, 1, 1)),
    )
    facts = []
    for quarter, (start, end) in enumerate(pairs, start=1):
        name = "%s_2026_q%d" % (parent, quarter)
        facts.append(_leaf(name, parent, "RANGE", {"from": start, "to": end},
                           level=2))
    return tuple(facts)


def _daily_leaf_facts(parent: str, start: datetime,
                      count: int) -> Tuple[ExpectedSpecFacts, ...]:
    facts = []
    for offset in range(count):
        day = start + timedelta(days=offset)
        name = "%s_%s" % (parent, day.strftime("%Y_%m_%d"))
        facts.append(_leaf(name, parent, "RANGE",
                           {"from": day, "to": day + timedelta(days=1)},
                           level=2))
    return tuple(facts)


_JOBS_FLAT_MONTHLY = PostgresPartitionPlan(
    parent="job_logs",
    method="RANGE",
    columns=("attempt_created_at",),
    policy=RangePolicy(
        columns=("attempt_created_at",),
        start=_dt(2026, 9, 1),
        interval="month",
        count=2,
    ),
)

_AUDIT_FLAT_EXPLICIT = PostgresPartitionPlan(
    parent="audit_events",
    method="RANGE",
    columns=("created_at",),
    policy=RangePolicy(
        columns=("created_at",),
        bounds=(
            (_dt(2026, 1, 1), _dt(2026, 1, 16)),
            (_dt(2026, 1, 16), _dt(2026, 2, 1)),
        ),
    ),
)

_EVENTS_FLAT_LIST = PostgresPartitionPlan(
    parent="events",
    method="LIST",
    columns=("region",),
    policy=ListPolicy(
        columns=("region",),
        groups=(("us", "ca"), ("eu",), ("apac",)),
    ),
)

_SESSIONS_FLAT_HASH = PostgresPartitionPlan(
    parent="sessions",
    method="HASH",
    columns=("tenant_id",),
    policy=HashPolicy(
        columns=("tenant_id",),
        modulus=4,
        remainders=(0, 2),
    ),
)

_AUDIT_RANGE_BRANCHES = PostgresPartitionPlan(
    parent="audit",
    method="RANGE",
    columns=("created_at",),
    branches=(
        RangeBranch(
            _dt(2026, 1, 1),
            _dt(2026, 7, 1),
            child=RangePolicy(
                columns=("created_at",),
                start=_dt(2026, 1, 1),
                interval="month",
                count=2,
            ),
            name="h1",
        ),
        RangeBranch(_dt(2026, 7, 1), _dt(2027, 1, 1), name="h2"),
    ),
)

_ORDERS_LIST_BRANCHES = PostgresPartitionPlan(
    parent="orders",
    method="LIST",
    columns=("status",),
    branches=(
        ListBranch(
            ("paid", "shipped"),
            child=ListPolicy(
                columns=("region",),
                groups=(("eu",), ("us",)),
            ),
            name="closed",
        ),
        ListBranch(("cancelled",), name="cancelled"),
    ),
)

_TELEMETRY_BRANCHES = (
    HashBranch(
        0,
        RangePolicy(
            columns=("created_at",),
            start=_dt(2026, 1, 1),
            interval="month",
            count=12,
        ),
    ),
    HashBranch(
        1,
        RangePolicy(
            columns=("created_at",),
            start=_dt(2026, 1, 1),
            interval="quarter",
            count=4,
        ),
    ),
    HashBranch(
        2,
        RangePolicy(
            columns=("created_at",),
            start=_dt(2026, 2, 1),
            interval="day",
            count=28,
        ),
    ),
)


CASES = [
    PartitionSyntaxCase(
        name="flat_range_generated",
        description=(
            "Flat RANGE parent with one monthly policy: two generated, "
            "half-open [start, end) leaves."
        ),
        plan=_JOBS_FLAT_MONTHLY,
        expected_specs=_monthly_leaf_facts("job_logs", 2026, 9, 2, level=1),
        required_fragments=(
            'PARTITION OF "job_logs"',
            "job_logs_2026_10",
            "FOR VALUES FROM ('2026-09-01 00:00:00+00:00') "
            "TO ('2026-10-01 00:00:00+00:00')",
        ),
        forbidden_fragments=(
            "PARTITION BY", "FOR VALUES IN", "MODULUS", "DEFAULT",
        ),
        drop_before=DropBeforeExpectation(
            cutoff=_dt(2026, 10, 1),
            candidates=("job_logs_2026_09",),
        ),
    ),
    PartitionSyntaxCase(
        name="flat_range_explicit",
        description=(
            "Flat RANGE parent with explicit half-month bounds; explicit "
            "bounds win over the default interval arithmetic."
        ),
        plan=_AUDIT_FLAT_EXPLICIT,
        expected_specs=(
            _leaf("audit_events_2026_01_01", "audit_events", "RANGE",
                  {"from": _dt(2026, 1, 1), "to": _dt(2026, 1, 16)}),
            _leaf("audit_events_2026_01_16", "audit_events", "RANGE",
                  {"from": _dt(2026, 1, 16), "to": _dt(2026, 2, 1)}),
        ),
        required_fragments=(
            'PARTITION OF "audit_events"',
            "audit_events_2026_01_16",
            "FOR VALUES FROM ('2026-01-01 00:00:00+00:00') "
            "TO ('2026-01-16 00:00:00+00:00')",
        ),
        forbidden_fragments=(
            "PARTITION BY", "FOR VALUES IN", "MODULUS", "DEFAULT",
        ),
        drop_before=DropBeforeExpectation(
            cutoff=_dt(2026, 1, 16),
            candidates=("audit_events_2026_01_01",),
        ),
    ),
    PartitionSyntaxCase(
        name="flat_list_groups",
        description=(
            "Flat LIST parent whose policy yields one partition per value "
            "group, with multi-value groups rendered as IN (...)."
        ),
        plan=_EVENTS_FLAT_LIST,
        expected_specs=(
            _leaf("events_us_ca", "events", "LIST", {"values": ("us", "ca")}),
            _leaf("events_eu", "events", "LIST", {"values": ("eu",)}),
            _leaf("events_apac", "events", "LIST", {"values": ("apac",)}),
        ),
        required_fragments=(
            'PARTITION OF "events"',
            "FOR VALUES IN ('us', 'ca')",
            "events_apac",
        ),
        forbidden_fragments=(
            "PARTITION BY", "FOR VALUES FROM", "MODULUS", "DEFAULT",
        ),
    ),
    PartitionSyntaxCase(
        name="flat_hash_remainders",
        description=(
            "Flat HASH parent selecting a subset of remainders from a "
            "modulus; buckets are named p<n> and rendered as "
            "MODULUS/REMAINDER."
        ),
        plan=_SESSIONS_FLAT_HASH,
        expected_specs=(
            _branch("sessions_p0", "sessions", "HASH",
                    {"modulus": 4, "remainder": 0}),
            _branch("sessions_p2", "sessions", "HASH",
                    {"modulus": 4, "remainder": 2}),
        ),
        required_fragments=(
            'PARTITION OF "sessions"',
            "FOR VALUES WITH (MODULUS 4, REMAINDER 0)",
            "FOR VALUES WITH (MODULUS 4, REMAINDER 2)",
        ),
        forbidden_fragments=(
            "PARTITION BY", "FOR VALUES FROM", "FOR VALUES IN", "DEFAULT",
        ),
    ),
    PartitionSyntaxCase(
        name="explicit_range_branches",
        description=(
            "First-level RANGE branches declared explicitly: one branch is "
            "subpartitioned again, the other is a leaf."
        ),
        plan=_AUDIT_RANGE_BRANCHES,
        expected_specs=(
            _branch("audit_h1", "audit", "RANGE",
                    {"from": _dt(2026, 1, 1), "to": _dt(2026, 7, 1)},
                    child_method="RANGE", child_columns=("created_at",)),
            _leaf("audit_h1_2026_01", "audit_h1", "RANGE",
                  {"from": _dt(2026, 1, 1), "to": _dt(2026, 2, 1)}, level=2),
            _leaf("audit_h1_2026_02", "audit_h1", "RANGE",
                  {"from": _dt(2026, 2, 1), "to": _dt(2026, 3, 1)}, level=2),
            _branch("audit_h2", "audit", "RANGE",
                    {"from": _dt(2026, 7, 1), "to": _dt(2027, 1, 1)}),
        ),
        required_fragments=(
            'PARTITION OF "audit"',
            "FOR VALUES FROM ('2026-01-01 00:00:00+00:00') "
            "TO ('2026-07-01 00:00:00+00:00')",
            'PARTITION BY RANGE ("created_at")',
            "audit_h1_2026_02",
        ),
        forbidden_fragments=("FOR VALUES IN", "MODULUS", "DEFAULT"),
        drop_before=DropBeforeExpectation(
            cutoff=_dt(2026, 7, 1),
            candidates=("audit_h1_2026_01", "audit_h1_2026_02"),
        ),
    ),
    PartitionSyntaxCase(
        name="explicit_list_branches",
        description=(
            "First-level LIST branches declared explicitly: a multi-value "
            "branch is subpartitioned by LIST, and a single-value branch is "
            "a leaf."
        ),
        plan=_ORDERS_LIST_BRANCHES,
        expected_specs=(
            _branch("orders_closed", "orders", "LIST",
                    {"values": ("paid", "shipped")},
                    child_method="LIST", child_columns=("region",)),
            _leaf("orders_closed_eu", "orders_closed", "LIST",
                  {"values": ("eu",)}, level=2),
            _leaf("orders_closed_us", "orders_closed", "LIST",
                  {"values": ("us",)}, level=2),
            _branch("orders_cancelled", "orders", "LIST",
                    {"values": ("cancelled",)}),
        ),
        required_fragments=(
            'PARTITION OF "orders"',
            "FOR VALUES IN ('paid', 'shipped') "
            'PARTITION BY LIST ("region")',
            "orders_closed_eu",
            "orders_cancelled",
        ),
        forbidden_fragments=("FOR VALUES FROM", "MODULUS", "DEFAULT"),
    ),
    PartitionSyntaxCase(
        name="hash_hetero_month_quarter_day_leaf",
        description=(
            "HASH modulus 4 parent whose first three branches choose monthly, "
            "quarterly and daily RANGE leaves; remainder 3 is a first-level "
            "leaf with no PARTITION BY."
        ),
        plan=PostgresPartitionPlan(
            parent="telemetry",
            method="HASH",
            columns=("tenant_bucket",),
            modulus=4,
            branches=(
                _TELEMETRY_BRANCHES[0],
                _TELEMETRY_BRANCHES[1],
                _TELEMETRY_BRANCHES[2],
                HashBranch(3),
            ),
        ),
        expected_specs=(
            _branch("telemetry_p0", "telemetry", "HASH",
                    {"modulus": 4, "remainder": 0},
                    child_method="RANGE", child_columns=("created_at",)),
            *_monthly_leaf_facts("telemetry_p0", 2026, 1, 12),
            _branch("telemetry_p1", "telemetry", "HASH",
                    {"modulus": 4, "remainder": 1},
                    child_method="RANGE", child_columns=("created_at",)),
            *_quarterly_leaf_facts("telemetry_p1"),
            _branch("telemetry_p2", "telemetry", "HASH",
                    {"modulus": 4, "remainder": 2},
                    child_method="RANGE", child_columns=("created_at",)),
            *_daily_leaf_facts("telemetry_p2", _dt(2026, 2, 1), 28),
            _branch("telemetry_p3", "telemetry", "HASH",
                    {"modulus": 4, "remainder": 3}),
        ),
        required_fragments=(
            'PARTITION OF "telemetry"',
            "FOR VALUES WITH (MODULUS 4, REMAINDER 3)",
            'PARTITION BY RANGE ("created_at")',
            "telemetry_p0_2026_01",
            "telemetry_p1_2026_q1",
            "telemetry_p2_2026_02_28",
        ),
        forbidden_fragments=("PARTITION BY LIST", "FOR VALUES IN", "DEFAULT"),
        drop_before=DropBeforeExpectation(
            cutoff=_dt(2026, 2, 2),
            candidates=("telemetry_p0_2026_01", "telemetry_p2_2026_02_01"),
        ),
        branch_leaf_counts=(
            ("telemetry_p0", 12),
            ("telemetry_p1", 4),
            ("telemetry_p2", 28),
            ("telemetry_p3", 0),
        ),
    ),
    PartitionSyntaxCase(
        name="hash_hetero_month_quarter_day_list",
        description=(
            "HASH modulus 4 parent whose first three branches choose RANGE "
            "granularities and whose remainder 3 branch is subpartitioned by "
            "LIST on a second column."
        ),
        plan=PostgresPartitionPlan(
            parent="telemetry",
            method="HASH",
            columns=("tenant_bucket",),
            modulus=4,
            branches=(
                _TELEMETRY_BRANCHES[0],
                _TELEMETRY_BRANCHES[1],
                _TELEMETRY_BRANCHES[2],
                HashBranch(
                    3,
                    ListPolicy(
                        columns=("region",),
                        groups=(("eu",), ("us",)),
                    ),
                ),
            ),
        ),
        expected_specs=(
            _branch("telemetry_p0", "telemetry", "HASH",
                    {"modulus": 4, "remainder": 0},
                    child_method="RANGE", child_columns=("created_at",)),
            *_monthly_leaf_facts("telemetry_p0", 2026, 1, 12),
            _branch("telemetry_p1", "telemetry", "HASH",
                    {"modulus": 4, "remainder": 1},
                    child_method="RANGE", child_columns=("created_at",)),
            *_quarterly_leaf_facts("telemetry_p1"),
            _branch("telemetry_p2", "telemetry", "HASH",
                    {"modulus": 4, "remainder": 2},
                    child_method="RANGE", child_columns=("created_at",)),
            *_daily_leaf_facts("telemetry_p2", _dt(2026, 2, 1), 28),
            _branch("telemetry_p3", "telemetry", "HASH",
                    {"modulus": 4, "remainder": 3},
                    child_method="LIST", child_columns=("region",)),
            _leaf("telemetry_p3_eu", "telemetry_p3", "LIST",
                  {"values": ("eu",)}, level=2),
            _leaf("telemetry_p3_us", "telemetry_p3", "LIST",
                  {"values": ("us",)}, level=2),
        ),
        required_fragments=(
            'PARTITION OF "telemetry"',
            "FOR VALUES WITH (MODULUS 4, REMAINDER 3) "
            'PARTITION BY LIST ("region")',
            "telemetry_p3_eu",
            "telemetry_p3_us",
            "FOR VALUES IN ('eu')",
        ),
        forbidden_fragments=("DEFAULT",),
        drop_before=DropBeforeExpectation(
            cutoff=_dt(2026, 2, 2),
            candidates=("telemetry_p0_2026_01", "telemetry_p2_2026_02_01"),
        ),
        branch_leaf_counts=(
            ("telemetry_p0", 12),
            ("telemetry_p1", 4),
            ("telemetry_p2", 28),
            ("telemetry_p3", 2),
        ),
    ),
    PartitionSyntaxCase(
        name="attach_detach",
        description=(
            "Statement-only case: ALTER TABLE ... ATTACH PARTITION for RANGE "
            "bounds and DETACH PARTITION CONCURRENTLY on PostgreSQL 14+."
        ),
        clause=ClauseCase(
            statements=(
                ClauseStatement(
                    kind="attach",
                    build=lambda dialect: PostgresAttachPartitionStatement(
                        dialect=dialect,
                        partition_name="orders_2026_q1",
                        parent_table="orders",
                        partition_type="RANGE",
                        partition_values={
                            "from": date(2026, 1, 1),
                            "to": date(2026, 4, 1),
                        },
                    ),
                    expected_sql=(
                        'ALTER TABLE "orders" ATTACH PARTITION '
                        '"orders_2026_q1" '
                        "FOR VALUES FROM ('2026-01-01') TO ('2026-04-01')"
                    ),
                ),
                ClauseStatement(
                    kind="detach",
                    build=lambda dialect: PostgresDetachPartitionStatement(
                        dialect=dialect,
                        partition_name="orders_2024",
                        parent_table="orders",
                        concurrently=True,
                    ),
                    expected_sql=(
                        'ALTER TABLE "orders" DETACH PARTITION '
                        '"orders_2024" CONCURRENTLY'
                    ),
                ),
            ),
            concurrent_attach_rejected=True,
        ),
        required_fragments=(
            "ATTACH PARTITION",
            "DETACH PARTITION",
            "CONCURRENTLY",
        ),
        forbidden_fragments=("CREATE TABLE", "PARTITION BY", "PARTITION OF"),
    ),
]


@pytest.fixture(params=CASES, ids=[case.name for case in CASES])
def partition_syntax_case(request):
    """Yield one ``PartitionSyntaxCase`` per parameterized matrix row."""
    return request.param
