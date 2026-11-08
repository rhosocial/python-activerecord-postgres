# tests/rhosocial/activerecord_postgres_test/feature/backend/test_partition_multilevel.py
"""Live PostgreSQL tests for multi-level, heterogeneous partitioning.

The tree under test is PostgreSQL's own capability, not a lowest-common-
denominator shape: a HASH parent whose four branches each pick a different
RANGE granularity.

    logs (HASH tenant_bucket, modulus 4)
    |-- p0  RANGE created_at, monthly, 12 leaves
    |-- p1  RANGE created_at, quarterly, 4 leaves
    |-- p2  RANGE created_at, yearly, 2 leaves
    `-- p3  RANGE created_at, explicit half-month spans, 24 leaves

These tests require a live PostgreSQL 11+ server. They prove:

* each level's DDL is accepted by the server (not only rendered);
* a row is routed by HASH to a branch and by RANGE to the correct leaf
  (``tableoid``), with ``[start, end)`` boundaries and explicit UTC bounds;
* the parent primary key must contain every level's partition key, and a
  server rejection is pinned as a negative case;
* maintenance is idempotent, reports missing/orphan partitions, drops only
  leaves, evaluates the cutoff per branch, and removes a whole branch only
  through the explicit ``drop_branch`` call.

Unit coverage for the same plan without a server lives in
``postgres/test_partition_plan.py``.
"""

import re
from datetime import datetime, timezone

import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.errors import DatabaseError
from rhosocial.activerecord.backend.expression import Column, Literal
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.statements import (
    ColumnConstraint,
    ColumnDefinition,
    ColumnNotNullConstraint,
    CreateTableStatement,
    DropTableStatement,
    InsertStatement,
    PartitionClause,
    PartitionStrategy,
    TableConstraint,
    TablePrimaryKeyConstraint,
    ValuesSource,
)
from rhosocial.activerecord.backend.expression.types import (
    BigIntType,
    TextType,
    TimestampTzType,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.partition import (
    PostgresCreatePartitionStatement,
)
from rhosocial.activerecord.backend.impl.postgres.partition import (
    AsyncPostgresPartitionMaintenance,
    HashBranch,
    PostgresPartitionMaintenance,
    PostgresPartitionPlan,
    RangePolicy,
)

TABLE = "ar_partition_multi_logs"
BAD_TABLE = "ar_partition_multi_badlogs"
ORPHAN = "ar_partition_multi_logs_p0_2019_01"

UTC = timezone.utc

P0 = "ar_partition_multi_logs_p0"
P1 = "ar_partition_multi_logs_p1"
P2 = "ar_partition_multi_logs_p2"
P3 = "ar_partition_multi_logs_p3"


def _dt(year, month, day, hour=0, minute=0, second=0, microsecond=0):
    return datetime(year, month, day, hour, minute, second, microsecond, tzinfo=UTC)


def _half_month_bounds():
    """Two spans per month: [1st, 16th) and [16th, next 1st)."""
    bounds = []
    for month in range(1, 13):
        first = _dt(2026, month, 1)
        middle = _dt(2026, month, 16)
        following = _dt(2027, 1, 1) if month == 12 else _dt(2026, month + 1, 1)
        bounds.append((first, middle))
        bounds.append((middle, following))
    return tuple(bounds)


def _plan():
    return PostgresPartitionPlan(
        parent=TABLE,
        method="HASH",
        columns=("tenant_bucket",),
        modulus=4,
        branches=(
            HashBranch(0, RangePolicy(
                columns=("created_at",), start=_dt(2026, 1, 1),
                interval="month", count=12)),
            HashBranch(1, RangePolicy(
                columns=("created_at",), start=_dt(2026, 1, 1),
                interval="quarter", count=4)),
            HashBranch(2, RangePolicy(
                columns=("created_at",), start=_dt(2026, 1, 1),
                interval="year", count=2)),
            HashBranch(3, RangePolicy(
                columns=("created_at",), bounds=_half_month_bounds())),
        ),
    )


def _parent_columns(dialect):
    return [
        ColumnDefinition(
            dialect, "id", BigIntType(dialect=dialect),
            constraints=[ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect))],
        ),
        ColumnDefinition(
            dialect, "tenant_bucket", BigIntType(dialect=dialect),
            constraints=[ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect))],
        ),
        ColumnDefinition(
            dialect, "created_at", TimestampTzType(dialect=dialect),
            constraints=[ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect))],
        ),
        ColumnDefinition(dialect, "payload", TextType(dialect=dialect)),
    ]


def _create_parent(backend, table, primary_key):
    dialect = backend.dialect
    statement = CreateTableStatement(
        dialect=dialect,
        table=Table(dialect, table),
        columns=_parent_columns(dialect),
        table_constraints=[
            TableConstraint(
                dialect, branch=TablePrimaryKeyConstraint(dialect, primary_key)
            )
        ],
        partition=PartitionClause(
            dialect, PartitionStrategy.HASH, [Column(dialect, "tenant_bucket")]
        ),
    )
    backend.execute(*statement.to_sql())


def _drop_tables(backend, *names):
    for name in names:
        statement = DropTableStatement(
            dialect=backend.dialect,
            table=Table(backend.dialect, name),
            if_exists=True,
            cascade=True,
        )
        backend.execute(*statement.to_sql())


def _insert_rows(backend, rows):
    dialect = backend.dialect
    statement = InsertStatement(
        dialect=dialect,
        into=Table(dialect, TABLE),
        columns=["id", "tenant_bucket", "created_at", "payload"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, value) for value in row] for row in rows],
        ),
    )
    backend.execute(*statement.to_sql())


def _require_multilevel(backend):
    dialect = backend.dialect
    if not dialect.supports_subpartitioning():
        pytest.skip("PostgreSQL server does not support subpartitioning")
    if not dialect.supports_hash_partitioning():
        pytest.skip("PostgreSQL server does not support HASH partitioning")


def _leaf_counts(report):
    counts = {P0: 0, P1: 0, P2: 0, P3: 0}
    for info in report.partitions:
        if info.level == 2:
            counts[info.parent] += 1
    return counts


def _probe_routing(backend):
    """Insert probe rows and return ``{branch_index: bucket}``.

    HASH partitioning uses ``hashint4``, not ``bucket % modulus``, so the
    mapping is read from the server rather than assumed. Every probe row must
    also satisfy ``satisfies_hash_partition`` for the branch it landed in.
    """
    _insert_rows(
        backend,
        [
            [100 + bucket, bucket, _dt(2026, 1, 15, 10, 0), "probe-%d" % bucket]
            for bucket in range(64)
        ],
    )
    check_sql = (
        "SELECT tenant_bucket, tableoid::regclass::text AS leaf, "
        "satisfies_hash_partition("
        "'public.%s'::regclass, 4, "
        "(regexp_match(tableoid::regclass::text, '_p(\\d+)_'))[1]::int, "
        "tenant_bucket) AS ok "
        "FROM %s WHERE id >= 100 AND id < 200" % (TABLE, TABLE)
    )
    rows = backend.fetch_all(check_sql)
    by_branch = {}
    for row in rows:
        assert row["ok"] is True, row
        branch = int(re.search(r"_p(\d+)_", row["leaf"]).group(1))
        by_branch[branch] = row["tenant_bucket"]
    assert sorted(by_branch) == [0, 1, 2, 3]
    return by_branch


def _assert_tree(report):
    root = next(info for info in report.partitions if info.level == 0)
    assert root.name == TABLE
    assert root.method == "HASH"
    branch_methods = {
        info.name: info.method for info in report.partitions if info.level == 1
    }
    assert branch_methods == {P0: "RANGE", P1: "RANGE", P2: "RANGE", P3: "RANGE"}
    assert _leaf_counts(report) == {P0: 12, P1: 4, P2: 2, P3: 24}
    assert report.missing == ()
    assert report.issues == ()


def _assert_primary_key(backend, table, expected_columns):
    rows = backend.fetch_all(
        "SELECT pg_get_constraintdef(c.oid) AS definition "
        "FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid "
        "WHERE t.relname = %s AND c.contype = 'p'",
        (table,),
    )
    assert len(rows) == 1
    definition = rows[0]["definition"]
    for column in expected_columns:
        assert column in definition


class TestPostgresPartitionMultilevel:
    """Synchronous live tests for a heterogeneous two-level tree."""

    @pytest.fixture
    def multilevel_plan(self, postgres_backend_single):
        backend = postgres_backend_single
        _require_multilevel(backend)
        _drop_tables(backend, TABLE, ORPHAN)
        _create_parent(backend, TABLE, ["id", "tenant_bucket", "created_at"])
        yield _plan()
        _drop_tables(backend, TABLE, ORPHAN)

    def test_creates_heterogeneous_nested_tree(self, postgres_backend_single, multilevel_plan):
        """The server accepts every branch's own RANGE granularity."""
        backend = postgres_backend_single
        maintenance = PostgresPartitionMaintenance(backend)
        report = maintenance.ensure(multilevel_plan)

        assert len(report.operations) == 46
        branch_sql = [op.sql for op in report.operations if op.level == 1]
        assert all("PARTITION BY RANGE" in sql for sql in branch_sql)
        assert all("FOR VALUES WITH (MODULUS 4" in sql for sql in branch_sql)
        leaf_sql = [op.sql for op in report.operations if op.level == 2]
        assert all("FOR VALUES FROM" in sql for sql in leaf_sql)

        _assert_tree(report)
        _assert_primary_key(
            backend, TABLE, ["id", "tenant_bucket", "created_at"]
        )

        again = maintenance.ensure(multilevel_plan)
        assert again.operations == ()
        _assert_tree(again)

    def test_routes_rows_by_hash_branch_then_range_leaf(
        self, postgres_backend_single, multilevel_plan
    ):
        """A row's branch follows the hash key and its leaf follows the bound."""
        backend = postgres_backend_single
        PostgresPartitionMaintenance(backend).ensure(multilevel_plan)
        buckets = _probe_routing(backend)

        boundary = [
            # branch, created_at, leaf suffix
            (0, _dt(2026, 1, 31, 23, 59, 59, 999999), "2026_01"),
            (0, _dt(2026, 2, 1, 0, 0), "2026_02"),
            (1, _dt(2026, 3, 31, 23, 59, 59, 999999), "2026_q1"),
            (1, _dt(2026, 4, 1, 0, 0), "2026_q2"),
            (2, _dt(2026, 12, 31, 23, 59, 59, 999999), "2026"),
            (2, _dt(2027, 1, 1, 0, 0), "2027"),
            (3, _dt(2026, 1, 15, 23, 59, 59, 999999), "2026_01_01"),
            (3, _dt(2026, 1, 16, 0, 0), "2026_01_16"),
        ]
        expected = {}
        rows = []
        for index, (branch, when, suffix) in enumerate(boundary):
            row_id = 200 + index
            expected[row_id] = "%s_p%d_%s" % (TABLE, branch, suffix)
            rows.append([row_id, buckets[branch], when, "boundary-%d" % index])
        _insert_rows(backend, rows)

        landed = backend.fetch_all(
            "SELECT id, tableoid::regclass::text AS leaf FROM %s "
            "WHERE id >= 200 AND id < 300 ORDER BY id" % TABLE
        )
        assert {row["id"]: row["leaf"] for row in landed} == expected

    def test_parent_primary_key_must_include_all_partition_keys(
        self, postgres_backend_single, multilevel_plan
    ):
        """A PK without the second-level key makes the server reject the tree."""
        backend = postgres_backend_single
        _drop_tables(backend, BAD_TABLE)
        try:
            _create_parent(backend, BAD_TABLE, ["id", "tenant_bucket"])
            statement = PostgresCreatePartitionStatement(
                dialect=backend.dialect,
                partition_name=BAD_TABLE + "_p0",
                parent_table=BAD_TABLE,
                partition_type="HASH",
                partition_values={"modulus": 4, "remainder": 0},
                partition_clause=PartitionClause(
                    backend.dialect,
                    PartitionStrategy.RANGE,
                    [Column(backend.dialect, "created_at")],
                ),
            )
            with pytest.raises(
                DatabaseError, match="must include all partitioning columns"
            ):
                backend.execute(*statement.to_sql())
        finally:
            _drop_tables(backend, BAD_TABLE)

    def test_list_reports_missing_leaf_and_orphan_branch(
        self, postgres_backend_single, multilevel_plan
    ):
        """list finds a dropped expected leaf and an extra catalog partition."""
        backend = postgres_backend_single
        maintenance = PostgresPartitionMaintenance(backend)
        maintenance.ensure(multilevel_plan)

        missing = P1 + "_2026_q2"
        backend.execute(
            *DropTableStatement(
                dialect=backend.dialect,
                table=Table(backend.dialect, missing),
                if_exists=True,
            ).to_sql()
        )
        backend.execute(
            *PostgresCreatePartitionStatement(
                dialect=backend.dialect,
                partition_name=ORPHAN,
                parent_table=P0,
                partition_type="RANGE",
                partition_values={
                    "from": _dt(2019, 1, 1),
                    "to": _dt(2019, 2, 1),
                },
            ).to_sql()
        )

        report = maintenance.list(multilevel_plan)
        assert report.missing == ((P1, missing),)
        assert ORPHAN in report.orphans
        assert report.issues == ()

        validate = maintenance.validate(multilevel_plan)
        assert validate.missing == ((P1, missing),)
        row = backend.fetch_one(
            "SELECT to_regclass(%s) AS present", (P1 + "_2026_q2",)
        )
        assert row["present"] is None

    def test_drop_before_evaluates_each_branch_policy(
        self, postgres_backend_single, multilevel_plan
    ):
        """One cutoff removes different counts per branch; dry run is silent."""
        backend = postgres_backend_single
        maintenance = PostgresPartitionMaintenance(backend)
        maintenance.ensure(multilevel_plan)
        cutoff = _dt(2026, 2, 1)

        planned = maintenance.drop_before(multilevel_plan, cutoff, dry_run=True)
        assert planned.dry_run is True
        assert [op.name for op in planned.operations] == [
            P0 + "_2026_01",
            P3 + "_2026_01_01",
            P3 + "_2026_01_16",
        ]
        assert P0 not in {op.name for op in planned.operations}

        dropped = maintenance.drop_before(multilevel_plan, cutoff, dry_run=False)
        assert [op.name for op in dropped.operations] == [
            op.name for op in planned.operations
        ]
        assert dropped.missing == ()
        remaining = {info.name for info in dropped.partitions}
        assert P0 + "_2026_02" in remaining
        assert P1 + "_2026_q1" in remaining
        assert P2 + "_2026" in remaining

        noop = maintenance.drop_before(multilevel_plan, cutoff, dry_run=False)
        assert noop.operations == ()

    def test_drop_branch_removes_one_branch_and_ensure_rebuilds_it(
        self, postgres_backend_single, multilevel_plan
    ):
        """drop_branch cascades its leaves; siblings and ensure are unaffected."""
        backend = postgres_backend_single
        maintenance = PostgresPartitionMaintenance(backend)
        maintenance.ensure(multilevel_plan)
        before = maintenance.list(multilevel_plan)

        report = maintenance.drop_branch(multilevel_plan, multilevel_plan.branches[0])
        assert [op.name for op in report.operations] == [P0]
        assert report.missing == ()
        remaining = {info.name for info in report.partitions}
        assert P0 not in remaining
        assert P0 + "_2026_01" not in remaining
        assert P1 in remaining
        assert len([info for info in report.partitions if info.parent == P1]) == 4

        rebuilt = maintenance.ensure(multilevel_plan)
        assert len(rebuilt.operations) == 13
        assert {op.name for op in rebuilt.operations} == {
            info.name for info in before.partitions if info.level > 0 and
            (info.name == P0 or info.parent == P0)
        }
        _assert_tree(rebuilt)


class TestAsyncPostgresPartitionMultilevel:
    """Asynchronous live tests; method names mirror the sync class."""

    @pytest_asyncio.fixture
    async def async_multilevel_plan(self, async_postgres_backend_single):
        backend = async_postgres_backend_single
        _require_multilevel(backend)
        for name in (TABLE, ORPHAN):
            statement = DropTableStatement(
                dialect=backend.dialect,
                table=Table(backend.dialect, name),
                if_exists=True,
                cascade=True,
            )
            await backend.execute(*statement.to_sql())
        dialect = backend.dialect
        statement = CreateTableStatement(
            dialect=dialect,
            table=Table(dialect, TABLE),
            columns=_parent_columns(dialect),
            table_constraints=[
                TableConstraint(
                    dialect,
                    branch=TablePrimaryKeyConstraint(
                        dialect, ["id", "tenant_bucket", "created_at"]
                    ),
                )
            ],
            partition=PartitionClause(
                dialect, PartitionStrategy.HASH, [Column(dialect, "tenant_bucket")]
            ),
        )
        await backend.execute(*statement.to_sql())
        yield _plan()
        for name in (TABLE, ORPHAN):
            drop = DropTableStatement(
                dialect=backend.dialect,
                table=Table(backend.dialect, name),
                if_exists=True,
                cascade=True,
            )
            await backend.execute(*drop.to_sql())

    @pytest.mark.asyncio
    async def test_creates_heterogeneous_nested_tree(
        self, async_postgres_backend_single, async_multilevel_plan
    ):
        """The async executor builds the same heterogeneous tree."""
        backend = async_postgres_backend_single
        maintenance = AsyncPostgresPartitionMaintenance(backend)
        report = await maintenance.ensure(async_multilevel_plan)
        assert len(report.operations) == 46
        _assert_tree(report)

        again = await maintenance.ensure(async_multilevel_plan)
        assert again.operations == ()

    @pytest.mark.asyncio
    async def test_drop_before_evaluates_each_branch_policy(
        self, async_postgres_backend_single, async_multilevel_plan
    ):
        """The async executor drops the same per-branch leaf set."""
        backend = async_postgres_backend_single
        maintenance = AsyncPostgresPartitionMaintenance(backend)
        await maintenance.ensure(async_multilevel_plan)
        planned = await maintenance.drop_before(
            async_multilevel_plan, _dt(2026, 2, 1), dry_run=True
        )
        assert [op.name for op in planned.operations] == [
            P0 + "_2026_01",
            P3 + "_2026_01_01",
            P3 + "_2026_01_16",
        ]
        assert planned.dry_run is True
