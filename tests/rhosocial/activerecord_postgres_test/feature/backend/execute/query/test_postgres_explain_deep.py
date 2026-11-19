# tests/rhosocial/activerecord_postgres_test/feature/backend/execute/query/test_postgres_explain_deep.py
"""Backend-local, deep EXPLAIN tests for PostgreSQL.

The shared testsuite contracts only assert that EXPLAIN *can run*. This file
goes further and interprets what PostgreSQL actually returns, which is
deliberately NOT generalised into the testsuite because dialects disagree on
plan shape. PostgreSQL has the richest EXPLAIN surface of the family, so the
coverage is split in two:

* **Construction level** (no database needed): the ``supports_explain_*``
  capability gates and the ``EXPLAIN (ANALYZE, FORMAT JSON, ...)`` SQL rendered
  by ``PostgresExplainMixin.format_explain_statement``. These run everywhere.
* **Result interpretation** (real PostgreSQL, CI service): the JSON plan tree
  (``Plan`` / ``Node Type`` / ``Relation Name``) is inspected for sequential
  versus index scans, aggregate/join nodes, and partition-pruning evidence.
  These tests use the same ``postgres_backend`` / ``async_postgres_backend``
  fixtures as the other backend-owned EXPLAIN tests in this directory and are
  skipped when no scenario is configured.

SQLite's local bytecode tests are intentionally NOT copied here -- they depend
on ``aiosqlite`` and on core's ``rhosocial.activerecord_test`` package, neither
of which this backend ships.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterator, List

import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.expression import (
    AllColumnsWildcard,
    QueryStatement,
    RawSQL,
)
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.statements import (
    ExplainFormat,
    ExplainOptions,
    ExplainStatement,
    ExplainType,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.explain import PostgresExplainResult

# ---------------------------------------------------------------------------
# Construction helpers (no database)
# ---------------------------------------------------------------------------


def _simple_query(dialect: PostgresDialect, table: str = "explain_deep_events") -> QueryStatement:
    """A minimal SELECT used purely to render an EXPLAIN statement."""
    return QueryStatement(
        dialect,
        select=[AllColumnsWildcard(dialect)],
        from_=Table(dialect, table),
    )


def _require_explain(dialect: PostgresDialect) -> None:
    if not dialect.supports_explain_plan():
        pytest.skip("dialect does not support EXPLAIN plans")


# ---------------------------------------------------------------------------
# JSON-plan helpers (real database)
# ---------------------------------------------------------------------------


def _parse_json_plan(raw_rows: List[Any]) -> Any:
    """Extract the JSON plan from rows returned by ``EXPLAIN (FORMAT JSON)``.

    psycopg may deserialize the ``json`` column automatically (list/dict) or
    return the raw JSON text, so accept both shapes. ``fetch_all`` yields dict
    rows keyed by ``QUERY PLAN``; the tuple branch mirrors the legacy row form.
    """
    if not raw_rows:
        return {}
    row = raw_rows[0]
    if isinstance(row, dict):
        data = row.get("QUERY PLAN", row)
    elif isinstance(row, (list, tuple)):
        data = row[0]
    else:
        data = row
    if isinstance(data, (list, dict)):
        return data
    return json.loads(data)


def _iter_nodes(plan: Any) -> Iterator[Dict[str, Any]]:
    """Walk a PostgreSQL JSON plan tree, yielding every node dict."""
    if isinstance(plan, list):
        for item in plan:
            yield from _iter_nodes(item)
    elif isinstance(plan, dict):
        if "Node Type" in plan or "Relation Name" in plan:
            yield plan
        for key in ("Plan", "Plans", "Subplans"):
            child = plan.get(key)
            if child is not None:
                yield from _iter_nodes(child)


def _node_types(plan: Any) -> List[str]:
    return [node["Node Type"] for node in _iter_nodes(plan) if "Node Type" in node]


def _relation_names(plan: Any) -> List[str]:
    return [node["Relation Name"] for node in _iter_nodes(plan) if "Relation Name" in node]


def _combined_text(result: PostgresExplainResult) -> str:
    return " ".join(str(row.get("QUERY PLAN", "")) for row in result.raw_rows).upper()


def _json_explain(backend: Any, sql: str) -> Any:
    return _parse_json_plan(backend.fetch_all(sql))


async def _async_json_explain(backend: Any, sql: str) -> Any:
    return _parse_json_plan(await backend.fetch_all(sql))


# ---------------------------------------------------------------------------
# Fixtures: a table with a primary key and a secondary index
# ---------------------------------------------------------------------------

_EVENTS_TABLE = "explain_deep_events"
_EVENTS_SETUP = [
    "DROP TABLE IF EXISTS explain_deep_events CASCADE",
    """CREATE TABLE explain_deep_events (
        id       SERIAL       PRIMARY KEY,
        category VARCHAR(32)  NOT NULL,
        payload  TEXT
    )""",
    "CREATE INDEX idx_explain_deep_category ON explain_deep_events(category)",
    """INSERT INTO explain_deep_events (category, payload)
        SELECT CASE WHEN n % 2 = 0 THEN 'alpha' ELSE 'beta' END, 'p' || n
        FROM generate_series(1, 200) AS seq(n)""",
    "ANALYZE explain_deep_events",
]
_EVENTS_CLEANUP = "DROP TABLE IF EXISTS explain_deep_events CASCADE"


@pytest.fixture
def indexed_backend(postgres_backend):
    for statement in _EVENTS_SETUP:
        postgres_backend.execute(statement)
    yield postgres_backend
    try:
        postgres_backend.execute(_EVENTS_CLEANUP)
    except Exception:
        pass


@pytest_asyncio.fixture
async def async_indexed_backend(async_postgres_backend):
    for statement in _EVENTS_SETUP:
        await async_postgres_backend.execute(statement)
    yield async_postgres_backend
    try:
        await async_postgres_backend.execute(_EVENTS_CLEANUP)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# 1. Capability gates + SQL rendering (construction level, no DB)
# ---------------------------------------------------------------------------


class TestPostgresExplainConstruction:
    """Verify ``supports_*`` gates agree with the SQL the dialect renders."""

    def test_capability_gates(self, postgres_dialect):
        """PostgreSQL advertises EXPLAIN, ANALYZE and the four native formats."""
        dialect = postgres_dialect
        assert dialect.supports_explain_plan() is True
        assert dialect.supports_explain_analyze() is True
        for fmt in ("TEXT", "JSON", "XML", "YAML"):
            assert dialect.supports_explain_format(fmt) is True, fmt
        assert dialect.supports_explain_format("NOT_A_FORMAT") is False

    def test_plain_explain_rendering(self, postgres_dialect):
        """With no options the renderer emits a bare ``EXPLAIN <statement>``."""
        dialect = postgres_dialect
        _require_explain(dialect)
        sql, params = ExplainStatement(dialect, _simple_query(dialect)).to_sql()
        assert sql.startswith("EXPLAIN SELECT")
        assert _EVENTS_TABLE in sql
        assert "(" not in sql, "no option list should be rendered without options"
        assert params == ()

    def test_analyze_rendering(self, postgres_dialect):
        """``ANALYZE`` is rendered only when the capability gate is open."""
        dialect = postgres_dialect
        _require_explain(dialect)
        if not dialect.supports_explain_analyze():
            pytest.skip("dialect does not support EXPLAIN ANALYZE")
        sql, _ = ExplainStatement(
            dialect,
            _simple_query(dialect),
            options=ExplainOptions(analyze=True),
        ).to_sql()
        assert sql.startswith("EXPLAIN (ANALYZE)")
        assert "ANALYZE" in sql

    def test_json_format_rendering(self, postgres_dialect):
        """``FORMAT JSON`` is rendered only when the gate accepts JSON."""
        dialect = postgres_dialect
        _require_explain(dialect)
        if not dialect.supports_explain_format("JSON"):
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        sql, _ = ExplainStatement(
            dialect,
            _simple_query(dialect),
            options=ExplainOptions(format=ExplainFormat.JSON),
        ).to_sql()
        assert sql.startswith("EXPLAIN (FORMAT JSON)")
        assert "FORMAT JSON" in sql
        assert "ANALYZE" not in sql

    def test_analyze_and_json_format_combined(self, postgres_dialect):
        """Options compose into a single parenthesised list in a stable order."""
        dialect = postgres_dialect
        _require_explain(dialect)
        if not dialect.supports_explain_analyze() or not dialect.supports_explain_format("JSON"):
            pytest.skip("dialect lacks ANALYZE or FORMAT JSON")
        sql, _ = ExplainStatement(
            dialect,
            _simple_query(dialect),
            options=ExplainOptions(analyze=True, format=ExplainFormat.JSON),
        ).to_sql()
        assert "EXPLAIN (ANALYZE, FORMAT JSON)" in sql

    def test_query_plan_type_is_omitted(self, postgres_dialect):
        """PostgreSQL has no QUERY PLAN keyword; plain EXPLAIN is equivalent."""
        dialect = postgres_dialect
        _require_explain(dialect)
        sql, _ = ExplainStatement(
            dialect,
            _simple_query(dialect),
            options=ExplainOptions(type=ExplainType.QUERY_PLAN),
        ).to_sql()
        assert sql.startswith("EXPLAIN SELECT")
        assert "QUERY PLAN" not in sql


# ---------------------------------------------------------------------------
# 2. Result interpretation via a real PostgreSQL server (sync)
# ---------------------------------------------------------------------------


class TestPostgresExplainResultInterpretation:
    """Interpret the JSON/text plan shapes PostgreSQL returns."""

    def test_text_plan_full_scan(self, indexed_backend):
        """An unfiltered SELECT plans as a Seq Scan with no index access."""
        dialect = indexed_backend.dialect
        _require_explain(dialect)
        result = indexed_backend.explain(RawSQL(dialect, "SELECT * FROM explain_deep_events"))
        assert isinstance(result, PostgresExplainResult)
        assert result.rows
        combined = _combined_text(result)
        assert "SEQ SCAN" in combined
        assert _EVENTS_TABLE.upper() in combined
        assert result.analyze_index_usage() == "full_scan"
        assert result.is_full_scan is True
        assert result.is_index_used is False

    def test_json_plan_exposes_plan_and_node_type(self, indexed_backend):
        """JSON output is a list whose first element carries ``Plan``/``Node Type``."""
        dialect = indexed_backend.dialect
        if not dialect.supports_explain_format("JSON"):
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        plan = _json_explain(
            indexed_backend,
            "EXPLAIN (FORMAT JSON) SELECT * FROM explain_deep_events",
        )
        assert isinstance(plan, list) and plan
        root = plan[0]["Plan"]
        assert "Node Type" in root
        assert root["Node Type"] == "Seq Scan"
        assert root.get("Relation Name") == _EVENTS_TABLE

    def test_json_plan_index_scan_when_seqscan_disabled(self, indexed_backend):
        """Forcing the planner off sequential scans yields an index access node."""
        dialect = indexed_backend.dialect
        if not dialect.supports_explain_format("JSON"):
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        indexed_backend.execute("SET enable_seqscan = off")
        try:
            plan = _json_explain(
                indexed_backend,
                "EXPLAIN (FORMAT JSON) SELECT * FROM explain_deep_events "
                "WHERE category = 'alpha'",
            )
        finally:
            indexed_backend.execute("RESET enable_seqscan")
        types = _node_types(plan)
        assert any("Index Scan" in node_type for node_type in types), types

    def test_json_plan_aggregate_node(self, indexed_backend):
        """A GROUP BY query produces an Aggregate-family node."""
        dialect = indexed_backend.dialect
        if not dialect.supports_explain_format("JSON"):
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        plan = _json_explain(
            indexed_backend,
            "EXPLAIN (FORMAT JSON) SELECT category, COUNT(*) "
            "FROM explain_deep_events GROUP BY category",
        )
        types = _node_types(plan)
        assert any("Aggregate" in node_type for node_type in types), types

    def test_json_plan_join_node(self, indexed_backend):
        """A two-way join produces a join node in the plan tree."""
        dialect = indexed_backend.dialect
        if not dialect.supports_explain_format("JSON"):
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        plan = _json_explain(
            indexed_backend,
            "EXPLAIN (FORMAT JSON) SELECT a.id FROM explain_deep_events a "
            "JOIN explain_deep_events b ON a.category = b.category",
        )
        types = _node_types(plan)
        assert any("Join" in node_type or node_type == "Nested Loop" for node_type in types), types


# ---------------------------------------------------------------------------
# 3. Result interpretation (async mirror of the key cases)
# ---------------------------------------------------------------------------


class TestAsyncPostgresExplainResultInterpretation:
    """Asynchronous counterparts of the core interpretation cases."""

    @pytest.mark.asyncio
    async def test_text_plan_full_scan(self, async_indexed_backend):
        dialect = async_indexed_backend.dialect
        _require_explain(dialect)
        result = await async_indexed_backend.explain(
            RawSQL(dialect, "SELECT * FROM explain_deep_events")
        )
        assert isinstance(result, PostgresExplainResult)
        assert result.rows
        assert "SEQ SCAN" in _combined_text(result)
        assert result.is_full_scan is True

    @pytest.mark.asyncio
    async def test_json_plan_exposes_node_type(self, async_indexed_backend):
        dialect = async_indexed_backend.dialect
        if not dialect.supports_explain_format("JSON"):
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        plan = await _async_json_explain(
            async_indexed_backend,
            "EXPLAIN (FORMAT JSON) SELECT * FROM explain_deep_events",
        )
        root = plan[0]["Plan"]
        assert root["Node Type"] == "Seq Scan"
        assert root.get("Relation Name") == _EVENTS_TABLE

    @pytest.mark.asyncio
    async def test_json_plan_aggregate_node(self, async_indexed_backend):
        dialect = async_indexed_backend.dialect
        if not dialect.supports_explain_format("JSON"):
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        plan = await _async_json_explain(
            async_indexed_backend,
            "EXPLAIN (FORMAT JSON) SELECT category, COUNT(*) "
            "FROM explain_deep_events GROUP BY category",
        )
        assert any("Aggregate" in t for t in _node_types(plan))


# ---------------------------------------------------------------------------
# 4. Partition-pruning evidence via a real PostgreSQL server
# ---------------------------------------------------------------------------

_PART_TABLE = "ar_pg_explain_deep_events"
_PARTITIONS = {
    "p2026_01": ("ar_pg_explain_deep_events_p2026_01", "2026-01-01", "2026-02-01"),
    "p2026_02": ("ar_pg_explain_deep_events_p2026_02", "2026-02-01", "2026-03-01"),
    "p2026_03": ("ar_pg_explain_deep_events_p2026_03", "2026-03-01", "2026-04-01"),
}


def _partition_setup_sql() -> List[str]:
    statements = ["DROP TABLE IF EXISTS %s CASCADE" % _PART_TABLE]
    for name, _, _ in _PARTITIONS.values():
        statements.append("DROP TABLE IF EXISTS %s CASCADE" % name)
    statements.append("""CREATE TABLE %s (
            id         BIGINT       NOT NULL,
            created_at TIMESTAMP    NOT NULL,
            tenant_id  BIGINT       NOT NULL,
            payload    TEXT,
            PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at)""" % _PART_TABLE)
    for name, start, end in _PARTITIONS.values():
        statements.append(
            "CREATE TABLE %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')"
            % (name, _PART_TABLE, start, end)
        )
    statements.append("""INSERT INTO %s (id, created_at, tenant_id, payload) VALUES
            (1, TIMESTAMP '2026-01-15', 100, 'jan'),
            (2, TIMESTAMP '2026-02-15', 100, 'feb'),
            (3, TIMESTAMP '2026-03-15', 200, 'mar')""" % _PART_TABLE)
    statements.append("ANALYZE %s" % _PART_TABLE)
    return statements


def _partition_cleanup_sql() -> List[str]:
    return [
        "DROP TABLE IF EXISTS %s CASCADE" % name
        for name in [_PART_TABLE] + [name for name, _, _ in _PARTITIONS.values()]
    ]


def _require_partition_support(dialect) -> None:
    _require_explain(dialect)
    if not dialect.supports_partitioned_table_creation():
        pytest.skip("server does not support declarative partitioning")
    if not dialect.supports_partitioned_table_primary_key():
        pytest.skip("server does not support primary keys on partitioned tables")


@pytest.fixture
def partition_backend(postgres_backend):
    _require_partition_support(postgres_backend.dialect)
    for statement in _partition_setup_sql():
        postgres_backend.execute(statement)
    yield postgres_backend
    for statement in _partition_cleanup_sql():
        try:
            postgres_backend.execute(statement)
        except Exception:
            pass


@pytest_asyncio.fixture
async def async_partition_backend(async_postgres_backend):
    _require_partition_support(async_postgres_backend.dialect)
    for statement in _partition_setup_sql():
        await async_postgres_backend.execute(statement)
    yield async_postgres_backend
    for statement in _partition_cleanup_sql():
        try:
            await async_postgres_backend.execute(statement)
        except Exception:
            pass


class TestPostgresPartitionPruningEvidence:
    """Assert the JSON plan shows target partitions and pruning of others."""

    def test_range_predicate_prunes_unrelated_partitions(self, partition_backend):
        dialect = partition_backend.dialect
        if not dialect.supports_explain_format("JSON"):
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        plan = _json_explain(
            partition_backend,
            "EXPLAIN (FORMAT JSON) SELECT * FROM %s "
            "WHERE created_at >= TIMESTAMP '2026-02-01' "
            "AND created_at < TIMESTAMP '2026-03-01'" % _PART_TABLE,
        )
        names = _relation_names(plan)
        target = _PARTITIONS["p2026_02"][0]
        assert target in names, "target partition missing from plan: %r" % names
        assert _PARTITIONS["p2026_01"][0] not in names, "unrelated January partition not pruned"
        assert _PARTITIONS["p2026_03"][0] not in names, "unrelated March partition not pruned"

    def test_full_scan_lists_every_partition(self, partition_backend):
        dialect = partition_backend.dialect
        if not dialect.supports_explain_format("JSON"):
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        plan = _json_explain(
            partition_backend,
            "EXPLAIN (FORMAT JSON) SELECT * FROM %s" % _PART_TABLE,
        )
        names = _relation_names(plan)
        for partition_name, _, _ in _PARTITIONS.values():
            assert partition_name in names, (
                "partition %s missing from full-scan plan" % partition_name
            )


class TestAsyncPostgresPartitionPruningEvidence:
    """Asynchronous partition-pruning counterpart."""

    @pytest.mark.asyncio
    async def test_range_predicate_prunes_unrelated_partitions(self, async_partition_backend):
        dialect = async_partition_backend.dialect
        if not dialect.supports_explain_format("JSON"):
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        plan = await _async_json_explain(
            async_partition_backend,
            "EXPLAIN (FORMAT JSON) SELECT * FROM %s "
            "WHERE created_at >= TIMESTAMP '2026-02-01' "
            "AND created_at < TIMESTAMP '2026-03-01'" % _PART_TABLE,
        )
        names = _relation_names(plan)
        target = _PARTITIONS["p2026_02"][0]
        assert target in names, "target partition missing from plan: %r" % names
        assert _PARTITIONS["p2026_01"][0] not in names
        assert _PARTITIONS["p2026_03"][0] not in names
