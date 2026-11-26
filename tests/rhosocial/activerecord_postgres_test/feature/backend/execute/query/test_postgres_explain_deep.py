# tests/rhosocial/activerecord_postgres_test/feature/backend/execute/query/test_postgres_explain_deep.py
"""Backend-local, deep EXPLAIN tests for PostgreSQL.

The shared testsuite contracts only assert that EXPLAIN *can run*. This file
interprets what PostgreSQL actually returns, which is deliberately NOT
generalised into the testsuite because dialects disagree on plan shape.
PostgreSQL has the richest EXPLAIN surface of the family, so coverage is split:

* **Construction level** (no database needed): the ``supports_explain_*``
  capability gates and the ``EXPLAIN (ANALYZE, FORMAT JSON, ...)`` SQL rendered
  by ``PostgresExplainMixin.format_explain_statement``.
* **Result interpretation** (real PostgreSQL, CI service): the JSON plan tree
  (``Plan`` / ``Node Type`` / ``Relation Name``) is inspected for sequential
  scans, aggregate/join nodes, and partition-pruning evidence.

Every statement -- including DDL/DML and the EXPLAIN wrapper -- is built with
the expression system and passed to the backend as ``*expr.to_sql()`` or as a
``QueryStatement`` to ``backend.explain()``. Strings appear only as assertion
expected values; no hand-built SQL is ever handed to a backend API.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.expression import (
    AllColumnsWildcard,
    Column,
    ColumnConstraint,
    ColumnDefinition,
    ColumnNotNullConstraint,
    ComparisonPredicate,
    CreateIndexStatement,
    CreateTableStatement,
    DropTableStatement,
    InsertStatement,
    Literal,
    LogicalPredicate,
    QueryStatement,
    TableConstraint,
    TablePrimaryKeyConstraint,
    ValuesSource,
)
from rhosocial.activerecord.backend.expression.functions import count
from rhosocial.activerecord.backend.expression.objects import Index, Table
from rhosocial.activerecord.backend.expression.query_parts import (
    GroupByHavingClause,
    JoinClause,
    WhereClause,
)
from rhosocial.activerecord.backend.expression.sources import NamedRelationRef
from rhosocial.activerecord.backend.expression.statements import (
    ExplainFormat,
    ExplainOptions,
    ExplainStatement,
    ExplainType,
    PartitionByRangeClause,
)
from rhosocial.activerecord.backend.expression.types import (
    BigIntType,
    TextType,
    TimestampType,
    VarCharType,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.explain import PostgresExplainResult
from rhosocial.activerecord.backend.impl.postgres.expression import (
    PostgresCreatePartitionStatement,
)

# ---------------------------------------------------------------------------
# Expression builders (no hand-built SQL anywhere)
# ---------------------------------------------------------------------------


def _simple_query(dialect: PostgresDialect, table: str = "explain_deep_events") -> QueryStatement:
    return QueryStatement(
        dialect,
        select=[AllColumnsWildcard(dialect)],
        from_=NamedRelationRef(dialect, Table(dialect, table)),
    )


def _category_query(dialect: PostgresDialect) -> QueryStatement:
    return QueryStatement(
        dialect,
        select=[AllColumnsWildcard(dialect)],
        from_=NamedRelationRef(dialect, Table(dialect, "explain_deep_events")),
        where=WhereClause(
            dialect,
            condition=ComparisonPredicate(
                dialect, "=", Column(dialect, "category"), Literal(dialect, "alpha")
            ),
        ),
    )


def _aggregate_query(dialect: PostgresDialect) -> QueryStatement:
    return QueryStatement(
        dialect,
        select=[Column(dialect, "category"), count(dialect).as_("n")],
        from_=NamedRelationRef(dialect, Table(dialect, "explain_deep_events")),
        group_by_having=GroupByHavingClause(dialect, group_by=[Column(dialect, "category")]),
    )


def _join_query(dialect: PostgresDialect) -> QueryStatement:
    left = NamedRelationRef(dialect, Table(dialect, "explain_deep_events"), alias="a")
    right = NamedRelationRef(dialect, Table(dialect, "explain_deep_events"), alias="b")
    join = JoinClause(
        dialect,
        left_table=left,
        right_table=right,
        condition=ComparisonPredicate(
            dialect,
            "=",
            Column(dialect, "category", table="a"),
            Column(dialect, "category", table="b"),
        ),
    )
    return QueryStatement(dialect, select=[Column(dialect, "id", table="a")], from_=join)


def _explain_expr(
    dialect: PostgresDialect,
    statement: Any,
    options: Optional[ExplainOptions] = None,
) -> ExplainStatement:
    return ExplainStatement(dialect, statement=statement, options=options)


def _render(
    dialect: PostgresDialect, options: Optional[ExplainOptions] = None
) -> Tuple[str, tuple]:
    return _explain_expr(dialect, _simple_query(dialect), options).to_sql()


def _require_explain(dialect: PostgresDialect) -> None:
    if not dialect.supports_explain_plan():
        pytest.skip("dialect does not support EXPLAIN plans")


# ---------------------------------------------------------------------------
# JSON-plan helpers
# ---------------------------------------------------------------------------


def _parse_json_plan(raw_rows: List[Any]) -> Any:
    """Extract the JSON plan from rows returned by ``EXPLAIN (FORMAT JSON)``."""
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


def _json_plan(backend: Any, statement: Any) -> Any:
    """Run ``EXPLAIN (FORMAT JSON)`` built by the expression/dialect system.

    ``backend.explain()`` is used for text plans, but its result wrapper coerces
    each plan row to ``str`` and therefore cannot carry a JSON document. For the
    JSON plan we render the very same ``ExplainStatement`` through the dialect
    and pass ``*expr.to_sql()`` to ``fetch_all`` -- no SQL string is hand-built.
    """
    expr = _explain_expr(
        backend.dialect,
        statement,
        options=ExplainOptions(format=ExplainFormat.JSON),
    )
    return _parse_json_plan(backend.fetch_all(*expr.to_sql()))


async def _async_json_plan(backend: Any, statement: Any) -> Any:
    expr = _explain_expr(
        backend.dialect,
        statement,
        options=ExplainOptions(format=ExplainFormat.JSON),
    )
    return _parse_json_plan(await backend.fetch_all(*expr.to_sql()))


def _iter_nodes(plan: Any) -> Iterator[Dict[str, Any]]:
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


# ---------------------------------------------------------------------------
# DDL/DML builders for the indexed events table
# ---------------------------------------------------------------------------

_EVENTS_TABLE = "explain_deep_events"


def _drop_events(dialect) -> DropTableStatement:
    return DropTableStatement(dialect=dialect, table=Table(dialect, _EVENTS_TABLE), if_exists=True)


def _create_events(dialect) -> CreateTableStatement:
    return CreateTableStatement(
        dialect=dialect,
        table=Table(dialect, _EVENTS_TABLE),
        columns=[
            ColumnDefinition(
                dialect,
                "id",
                BigIntType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect))],
            ),
            ColumnDefinition(
                dialect,
                "category",
                VarCharType(dialect, 32),
                constraints=[ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect))],
            ),
        ],
        table_constraints=[
            TableConstraint(dialect, branch=TablePrimaryKeyConstraint(dialect, ["id"]))
        ],
    )


def _index_events(dialect) -> CreateIndexStatement:
    return CreateIndexStatement(
        dialect=dialect,
        index=Index(dialect, "idx_explain_deep_category"),
        table=Table(dialect, _EVENTS_TABLE),
        columns=["category"],
    )


def _seed_events(dialect) -> InsertStatement:
    rows = [[i, "alpha" if i % 2 == 0 else "beta"] for i in range(1, 201)]
    return InsertStatement(
        dialect=dialect,
        into=Table(dialect, _EVENTS_TABLE),
        columns=["id", "category"],
        source=ValuesSource(dialect, [[Literal(dialect, value) for value in row] for row in rows]),
    )


@pytest.fixture
def indexed_backend(postgres_backend):
    dialect = postgres_backend.dialect
    for expr in (
        _drop_events(dialect),
        _create_events(dialect),
        _index_events(dialect),
        _seed_events(dialect),
    ):
        postgres_backend.execute(*expr.to_sql())
    yield postgres_backend
    try:
        postgres_backend.execute(*_drop_events(dialect).to_sql())
    except Exception:
        pass


@pytest_asyncio.fixture
async def async_indexed_backend(async_postgres_backend):
    dialect = async_postgres_backend.dialect
    for expr in (
        _drop_events(dialect),
        _create_events(dialect),
        _index_events(dialect),
        _seed_events(dialect),
    ):
        await async_postgres_backend.execute(*expr.to_sql())
    yield async_postgres_backend
    try:
        await async_postgres_backend.execute(*_drop_events(dialect).to_sql())
    except Exception:
        pass


# ---------------------------------------------------------------------------
# 1. Capability gates + SQL rendering (construction level, no DB)
# ---------------------------------------------------------------------------


class TestPostgresExplainConstruction:
    def test_capability_gates(self, postgres_dialect):
        dialect = postgres_dialect
        assert dialect.supports_explain_plan() is True
        assert dialect.supports_explain_analyze() is True
        assert dialect.supports_explain_verbose() is True
        assert dialect.supports_explain_costs() is True
        assert dialect.supports_explain_buffers() is True
        assert dialect.supports_explain_timing() is True
        assert dialect.supports_explain_settings() is True
        assert dialect.supports_explain_wal() is True
        for probe in (
            "supports_explain_format_text",
            "supports_explain_format_json",
            "supports_explain_format_xml",
            "supports_explain_format_yaml",
        ):
            assert getattr(dialect, probe)() is True, probe
        for probe in (
            "supports_explain_format_tree",
            "supports_explain_format_traditional",
        ):
            assert getattr(dialect, probe)() is False, probe

    def test_plain_explain_rendering(self, postgres_dialect):
        dialect = postgres_dialect
        _require_explain(dialect)
        sql, params = _render(dialect)
        assert sql.startswith("EXPLAIN SELECT")
        assert _EVENTS_TABLE in sql
        assert "(" not in sql, "no option list should be rendered without options"
        assert params == ()

    def test_analyze_rendering(self, postgres_dialect):
        dialect = postgres_dialect
        _require_explain(dialect)
        if not dialect.supports_explain_analyze():
            pytest.skip("dialect does not support EXPLAIN ANALYZE")
        sql, _ = _render(dialect, ExplainOptions(analyze=True))
        assert sql.startswith("EXPLAIN (ANALYZE)")
        assert "ANALYZE" in sql

    def test_json_format_rendering(self, postgres_dialect):
        dialect = postgres_dialect
        _require_explain(dialect)
        if not dialect.supports_explain_format_json():
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        sql, _ = _render(dialect, ExplainOptions(format=ExplainFormat.JSON))
        assert sql.startswith("EXPLAIN (FORMAT JSON)")
        assert "FORMAT JSON" in sql
        assert "ANALYZE" not in sql

    def test_analyze_and_json_format_combined(self, postgres_dialect):
        dialect = postgres_dialect
        _require_explain(dialect)
        if not dialect.supports_explain_analyze() or not dialect.supports_explain_format_json():
            pytest.skip("dialect lacks ANALYZE or FORMAT JSON")
        sql, _ = _render(dialect, ExplainOptions(analyze=True, format=ExplainFormat.JSON))
        assert "EXPLAIN (ANALYZE, FORMAT JSON)" in sql

    def test_query_plan_type_is_omitted(self, postgres_dialect):
        dialect = postgres_dialect
        _require_explain(dialect)
        sql, _ = _render(dialect, ExplainOptions(type=ExplainType.QUERY_PLAN))
        assert sql.startswith("EXPLAIN SELECT")
        assert "QUERY PLAN" not in sql


# ---------------------------------------------------------------------------
# 2. Result interpretation via a real PostgreSQL server (sync)
# ---------------------------------------------------------------------------


class TestPostgresExplainResultInterpretation:
    def test_text_plan_full_scan(self, indexed_backend):
        dialect = indexed_backend.dialect
        _require_explain(dialect)
        result = indexed_backend.explain(_simple_query(dialect))
        assert isinstance(result, PostgresExplainResult)
        assert result.rows
        combined = _combined_text(result)
        assert "SEQ SCAN" in combined
        assert _EVENTS_TABLE.upper() in combined
        assert result.analyze_index_usage() == "full_scan"
        assert result.is_full_scan is True
        assert result.is_index_used is False

    def test_json_plan_exposes_plan_and_node_type(self, indexed_backend):
        dialect = indexed_backend.dialect
        if not dialect.supports_explain_format_json():
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        plan = _json_plan(indexed_backend, _simple_query(dialect))
        assert isinstance(plan, list) and plan
        root = plan[0]["Plan"]
        assert "Node Type" in root
        assert root["Node Type"] == "Seq Scan"
        assert root.get("Relation Name") == _EVENTS_TABLE

    def test_json_plan_category_filter(self, indexed_backend):
        """An indexed-column predicate still yields an interpretable JSON plan."""
        dialect = indexed_backend.dialect
        if not dialect.supports_explain_format_json():
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        plan = _json_plan(indexed_backend, _category_query(dialect))
        types = _node_types(plan)
        assert any("Scan" in node_type for node_type in types), types

    def test_json_plan_aggregate_node(self, indexed_backend):
        dialect = indexed_backend.dialect
        if not dialect.supports_explain_format_json():
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        plan = _json_plan(indexed_backend, _aggregate_query(dialect))
        types = _node_types(plan)
        assert any("Aggregate" in node_type for node_type in types), types

    def test_json_plan_join_node(self, indexed_backend):
        dialect = indexed_backend.dialect
        if not dialect.supports_explain_format_json():
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        plan = _json_plan(indexed_backend, _join_query(dialect))
        types = _node_types(plan)
        assert any("Join" in node_type or node_type == "Nested Loop" for node_type in types), types


# ---------------------------------------------------------------------------
# 3. Result interpretation (async mirror of the key cases)
# ---------------------------------------------------------------------------


class TestAsyncPostgresExplainResultInterpretation:
    @pytest.mark.asyncio
    async def test_text_plan_full_scan(self, async_indexed_backend):
        dialect = async_indexed_backend.dialect
        _require_explain(dialect)
        result = await async_indexed_backend.explain(_simple_query(dialect))
        assert isinstance(result, PostgresExplainResult)
        assert result.rows
        assert "SEQ SCAN" in _combined_text(result)
        assert result.is_full_scan is True

    @pytest.mark.asyncio
    async def test_json_plan_exposes_node_type(self, async_indexed_backend):
        dialect = async_indexed_backend.dialect
        if not dialect.supports_explain_format_json():
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        plan = await _async_json_plan(async_indexed_backend, _simple_query(dialect))
        root = plan[0]["Plan"]
        assert root["Node Type"] == "Seq Scan"
        assert root.get("Relation Name") == _EVENTS_TABLE

    @pytest.mark.asyncio
    async def test_json_plan_aggregate_node(self, async_indexed_backend):
        dialect = async_indexed_backend.dialect
        if not dialect.supports_explain_format_json():
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        plan = await _async_json_plan(async_indexed_backend, _aggregate_query(dialect))
        assert any("Aggregate" in t for t in _node_types(plan))


# ---------------------------------------------------------------------------
# 4. Partition-pruning evidence via a real PostgreSQL server
# ---------------------------------------------------------------------------

_PART_TABLE = "ar_pg_explain_deep_events"
_PARTITIONS = {
    "p2026_01": ("ar_pg_explain_deep_events_p2026_01", datetime(2026, 1, 1), datetime(2026, 2, 1)),
    "p2026_02": ("ar_pg_explain_deep_events_p2026_02", datetime(2026, 2, 1), datetime(2026, 3, 1)),
    "p2026_03": ("ar_pg_explain_deep_events_p2026_03", datetime(2026, 3, 1), datetime(2026, 4, 1)),
}


def _drop_partition(dialect, name: str) -> DropTableStatement:
    return DropTableStatement(dialect=dialect, table=Table(dialect, name), if_exists=True)


def _create_partition_parent(dialect) -> CreateTableStatement:
    return CreateTableStatement(
        dialect=dialect,
        table=Table(dialect, _PART_TABLE),
        columns=[
            ColumnDefinition(
                dialect,
                "id",
                BigIntType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect))],
            ),
            ColumnDefinition(
                dialect,
                "created_at",
                TimestampType(precision=6, dialect=dialect),
                constraints=[ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect))],
            ),
            ColumnDefinition(
                dialect,
                "tenant_id",
                BigIntType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect))],
            ),
            ColumnDefinition(dialect, "payload", TextType(dialect=dialect)),
        ],
        table_constraints=[
            TableConstraint(
                dialect, branch=TablePrimaryKeyConstraint(dialect, ["id", "created_at"])
            )
        ],
        partition=PartitionByRangeClause(
            dialect=dialect,
            keys=[Column(dialect, "created_at")],
        ),
    )


def _create_partition(dialect, name: str, start: datetime, end: datetime):
    return PostgresCreatePartitionStatement(
        dialect=dialect,
        partition_name=name,
        parent_table=_PART_TABLE,
        partition_type="RANGE",
        partition_values={
            "from": start.strftime("%Y-%m-%d"),
            "to": end.strftime("%Y-%m-%d"),
        },
    )


def _seed_partition_rows(dialect) -> InsertStatement:
    rows = [
        [1, datetime(2026, 1, 15), 100, "jan"],
        [2, datetime(2026, 2, 15), 100, "feb"],
        [3, datetime(2026, 3, 15), 200, "mar"],
    ]
    return InsertStatement(
        dialect=dialect,
        into=Table(dialect, _PART_TABLE),
        columns=["id", "created_at", "tenant_id", "payload"],
        source=ValuesSource(dialect, [[Literal(dialect, value) for value in row] for row in rows]),
    )


def _partition_range_query(dialect, start: datetime, end: datetime) -> QueryStatement:
    condition = LogicalPredicate(
        dialect,
        "AND",
        ComparisonPredicate(dialect, ">=", Column(dialect, "created_at"), Literal(dialect, start)),
        ComparisonPredicate(dialect, "<", Column(dialect, "created_at"), Literal(dialect, end)),
    )
    return QueryStatement(
        dialect,
        select=[AllColumnsWildcard(dialect)],
        from_=NamedRelationRef(dialect, Table(dialect, _PART_TABLE)),
        where=WhereClause(dialect, condition=condition),
    )


def _partition_full_scan_query(dialect) -> QueryStatement:
    return QueryStatement(
        dialect,
        select=[AllColumnsWildcard(dialect)],
        from_=NamedRelationRef(dialect, Table(dialect, _PART_TABLE)),
    )


def _partition_ddl(dialect):
    exprs = [_drop_partition(dialect, _PART_TABLE)]
    exprs.extend(_drop_partition(dialect, name) for name, _, _ in _PARTITIONS.values())
    exprs.append(_create_partition_parent(dialect))
    exprs.extend(
        _create_partition(dialect, name, start, end) for name, start, end in _PARTITIONS.values()
    )
    exprs.append(_seed_partition_rows(dialect))
    return exprs


def _partition_cleanup(dialect):
    exprs = [_drop_partition(dialect, name) for name, _, _ in _PARTITIONS.values()]
    exprs.append(_drop_partition(dialect, _PART_TABLE))
    return exprs


def _require_partition_support(dialect) -> None:
    _require_explain(dialect)
    if not dialect.supports_partitioned_table_creation():
        pytest.skip("server does not support declarative partitioning")
    if not dialect.supports_partitioned_table_primary_key():
        pytest.skip("server does not support primary keys on partitioned tables")


@pytest.fixture
def partition_backend(postgres_backend):
    dialect = postgres_backend.dialect
    _require_partition_support(dialect)
    for expr in _partition_ddl(dialect):
        postgres_backend.execute(*expr.to_sql())
    yield postgres_backend
    for expr in _partition_cleanup(dialect):
        try:
            postgres_backend.execute(*expr.to_sql())
        except Exception:
            pass


@pytest_asyncio.fixture
async def async_partition_backend(async_postgres_backend):
    dialect = async_postgres_backend.dialect
    _require_partition_support(dialect)
    for expr in _partition_ddl(dialect):
        await async_postgres_backend.execute(*expr.to_sql())
    yield async_postgres_backend
    for expr in _partition_cleanup(dialect):
        try:
            await async_postgres_backend.execute(*expr.to_sql())
        except Exception:
            pass


class TestPostgresPartitionPruningEvidence:
    def test_range_predicate_prunes_unrelated_partitions(self, partition_backend):
        dialect = partition_backend.dialect
        if not dialect.supports_explain_format_json():
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        plan = _json_plan(
            partition_backend,
            _partition_range_query(dialect, datetime(2026, 2, 1), datetime(2026, 3, 1)),
        )
        names = _relation_names(plan)
        target = _PARTITIONS["p2026_02"][0]
        assert target in names, "target partition missing from plan: %r" % names
        assert _PARTITIONS["p2026_01"][0] not in names, "unrelated January partition not pruned"
        assert _PARTITIONS["p2026_03"][0] not in names, "unrelated March partition not pruned"

    def test_full_scan_lists_every_partition(self, partition_backend):
        dialect = partition_backend.dialect
        if not dialect.supports_explain_format_json():
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        plan = _json_plan(partition_backend, _partition_full_scan_query(dialect))
        names = _relation_names(plan)
        for name, _, _ in _PARTITIONS.values():
            assert name in names, "partition %s missing from full-scan plan" % name


class TestAsyncPostgresPartitionPruningEvidence:
    @pytest.mark.asyncio
    async def test_range_predicate_prunes_unrelated_partitions(self, async_partition_backend):
        dialect = async_partition_backend.dialect
        if not dialect.supports_explain_format_json():
            pytest.skip("dialect does not support EXPLAIN FORMAT JSON")
        plan = await _async_json_plan(
            async_partition_backend,
            _partition_range_query(dialect, datetime(2026, 2, 1), datetime(2026, 3, 1)),
        )
        names = _relation_names(plan)
        target = _PARTITIONS["p2026_02"][0]
        assert target in names, "target partition missing from plan: %r" % names
        assert _PARTITIONS["p2026_01"][0] not in names
        assert _PARTITIONS["p2026_03"][0] not in names
