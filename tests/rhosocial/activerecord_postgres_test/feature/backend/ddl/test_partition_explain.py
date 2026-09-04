# tests/rhosocial/activerecord_postgres_test/feature/backend/ddl/test_partition_explain.py
"""PostgreSQL EXPLAIN tests for partitioned tables."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.expression import (
    Column,
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
    CreateTableExpression,
    DropTableExpression,
    InsertExpression,
    Literal,
    QueryExpression,
    TableExpression,
    ValuesSource,
    WildcardExpression,
)
from rhosocial.activerecord.backend.expression.statements import (
    ExplainExpression,
    ExplainFormat,
    ExplainOptions,
    PartitionClause,
    PartitionStrategy,
    TableConstraint,
    TableConstraintType,
)
from rhosocial.activerecord.backend.expression.types import BigIntType, TextType, TimestampType
from rhosocial.activerecord.backend.impl.postgres import PostgresExplainResult
from rhosocial.activerecord.backend.impl.postgres.expression import (
    PostgresCreatePartitionExpression,
)

PARTITION_EXPLAIN_TABLE = "ar_pg_partition_explain_events"
PARTITION_NAMES = {
    "p2026_01": ("ar_pg_partition_explain_events_p2026_01", "2026-01-01", "2026-02-01"),
    "p2026_02": ("ar_pg_partition_explain_events_p2026_02", "2026-02-01", "2026-03-01"),
    "p2026_03": ("ar_pg_partition_explain_events_p2026_03", "2026-03-01", "2026-04-01"),
}


def _all_tables():
    tables = [PARTITION_EXPLAIN_TABLE]
    for _, name, _, _ in _iter_partitions():
        tables.append(name)
    return tables


def _iter_partitions():
    for key, (name, from_val, to_val) in PARTITION_NAMES.items():
        yield key, name, from_val, to_val


def _drop_table_sql(dialect, table_name: str):
    return DropTableExpression(dialect=dialect, table=table_name, if_exists=True).to_sql()


def _create_parent_table_expression(dialect):
    return CreateTableExpression(
        dialect=dialect,
        table=PARTITION_EXPLAIN_TABLE,
        if_not_exists=False,
        columns=[
            ColumnDefinition("id", BigIntType(),
                             constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("created_at", TimestampType(precision=6),
                             constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("tenant_id", BigIntType(),
                             constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("payload", TextType()),
        ],
        table_constraints=[
            TableConstraint(
                TableConstraintType.PRIMARY_KEY,
                columns=["id", "created_at"],
            ),
        ],
        partition=PartitionClause(
            dialect=dialect,
            method=PartitionStrategy.RANGE,
            keys=[Column(dialect, "created_at")],
        ),
    )


def _create_partition_sql(dialect, partition_name: str, from_value: str, to_value: str):
    expr = PostgresCreatePartitionExpression(
        dialect=dialect,
        partition_name=partition_name,
        parent_table=PARTITION_EXPLAIN_TABLE,
        partition_type="RANGE",
        partition_values={"from": from_value, "to": to_value},
    )
    return expr.to_sql()


def _seed_rows_expression(dialect):
    rows = [
        [1, datetime(2026, 1, 15), 100, "jan"],
        [2, datetime(2026, 2, 15), 100, "feb"],
        [3, datetime(2026, 3, 15), 200, "mar"],
    ]
    return InsertExpression(
        dialect=dialect,
        into=PARTITION_EXPLAIN_TABLE,
        columns=["id", "created_at", "tenant_id", "payload"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, value) for value in row] for row in rows],
        ),
    )


def _range_query_expression(dialect, start, end):
    return QueryExpression(
        dialect,
        select=[WildcardExpression(dialect)],
        from_=TableExpression(dialect, PARTITION_EXPLAIN_TABLE),
        where=(Column(dialect, "created_at") >= Literal(dialect, start))
        & (Column(dialect, "created_at") < Literal(dialect, end)),
    )


def _full_scan_query_expression(dialect):
    return QueryExpression(
        dialect,
        select=[WildcardExpression(dialect)],
        from_=TableExpression(dialect, PARTITION_EXPLAIN_TABLE),
    )


def _json_explain_sql(dialect, query_expr):
    expr = ExplainExpression(
        dialect,
        query_expr,
        options=ExplainOptions(format=ExplainFormat.JSON),
    )
    return expr.to_sql()


def _setup_partition_table(backend):
    dialect = backend.dialect
    for table in _all_tables():
        backend.execute(*_drop_table_sql(dialect, table))
    backend.execute(*_create_parent_table_expression(dialect).to_sql())
    for _, name, from_val, to_val in _iter_partitions():
        backend.execute(*_create_partition_sql(dialect, name, from_val, to_val))
    backend.execute(*_seed_rows_expression(dialect).to_sql())


async def _async_setup_partition_table(backend):
    dialect = backend.dialect
    for table in _all_tables():
        await backend.execute(*_drop_table_sql(dialect, table))
    await backend.execute(*_create_parent_table_expression(dialect).to_sql())
    for _, name, from_val, to_val in _iter_partitions():
        await backend.execute(*_create_partition_sql(dialect, name, from_val, to_val))
    await backend.execute(*_seed_rows_expression(dialect).to_sql())


def _teardown_partition_table(backend):
    dialect = backend.dialect
    for table in reversed(_all_tables()):
        try:
            backend.execute(*_drop_table_sql(dialect, table))
        except Exception:
            pass


async def _async_teardown_partition_table(backend):
    dialect = backend.dialect
    for table in reversed(_all_tables()):
        try:
            await backend.execute(*_drop_table_sql(dialect, table))
        except Exception:
            pass


def _flatten_plan_text(plan: Any, lines: list) -> None:
    """Recursively extract text plan lines from a JSON EXPLAIN result."""
    if isinstance(plan, list):
        for item in plan:
            _flatten_plan_text(item, lines)
    elif isinstance(plan, dict):
        if "Plan" in plan:
            _flatten_plan_text(plan["Plan"], lines)
        for key, value in plan.items():
            if key in ("Plans", "Subplans"):
                for sub in value:
                    _flatten_plan_text(sub, lines)
            elif key == "Relation Name":
                lines.append(value)
    elif isinstance(plan, str):
        lines.append(plan)


def _parse_json_plan(result: PostgresExplainResult) -> Any:
    """Extract the JSON plan from a PostgresExplainResult.

    psycopg's JSON adapter may deserialize the column value automatically,
    so we accept both parsed (list/dict) and unparsed (str) values.
    """
    raw = result.raw_rows
    if raw and isinstance(raw[0], dict) and "QUERY PLAN" in raw[0]:
        plan_data = raw[0]["QUERY PLAN"]
        if isinstance(plan_data, (list, dict)):
            return plan_data
        import json
        return json.loads(plan_data)
    if raw and isinstance(raw[0], tuple):
        plan_data = raw[0][0]
        if isinstance(plan_data, (list, dict)):
            return plan_data
        import json
        return json.loads(plan_data)
    return {}


def _extract_relation_names(result: PostgresExplainResult) -> list[str]:
    """Extract partition relation names from a JSON-format EXPLAIN result."""
    plan = _parse_json_plan(result)
    names: list[str] = []
    _flatten_plan_text(plan, names)
    return names


def _assert_range_partition_support(dialect):
    if not dialect.supports_partitioned_table_creation():
        pytest.skip("PostgreSQL scenario does not support declarative partitioning")
    version = dialect.get_server_version()
    if version and version[0] < 11:
        pytest.skip("PostgreSQL 10 does not support primary keys on partitioned tables")


@pytest.fixture
def pg_partition_explain_backend(postgres_backend):
    """Create a range-partitioned table for EXPLAIN tests."""
    _assert_range_partition_support(postgres_backend.dialect)
    _setup_partition_table(postgres_backend)
    yield postgres_backend
    _teardown_partition_table(postgres_backend)


@pytest_asyncio.fixture
async def async_pg_partition_explain_backend(async_postgres_backend):
    """Async counterpart for partition explain tests."""
    _assert_range_partition_support(async_postgres_backend.dialect)
    await _async_setup_partition_table(async_postgres_backend)
    yield async_postgres_backend
    await _async_teardown_partition_table(async_postgres_backend)


def _combined_plan_text(result: PostgresExplainResult) -> str:
    """Join all lines of the EXPLAIN TEXT output into a single string."""
    lines = []
    for row in result.raw_rows:
        if isinstance(row, dict):
            for val in row.values():
                lines.append(str(val))
        elif isinstance(row, tuple):
            for val in row:
                lines.append(str(val))
        else:
            lines.append(str(row))
    return "\n".join(lines)


class TestPostgresPartitionExplain:
    """Synchronous EXPLAIN tests for PostgreSQL partition pruning."""

    def test_explain_partition_pruning_on_range_predicate(
        self,
        pg_partition_explain_backend,
    ):
        """Range predicate on partition key should prune unrelated partitions."""
        dialect = pg_partition_explain_backend.dialect
        query = _range_query_expression(
            dialect,
            datetime(2026, 2, 1),
            datetime(2026, 3, 1),
        )
        result = pg_partition_explain_backend.explain(query)
        assert isinstance(result, PostgresExplainResult)
        assert len(result.raw_rows) > 0

    def test_explain_json_reports_target_partition(
        self,
        pg_partition_explain_backend,
    ):
        """EXPLAIN FORMAT JSON should expose the pruned target partition name."""
        dialect = pg_partition_explain_backend.dialect
        query = _range_query_expression(
            dialect,
            datetime(2026, 2, 1),
            datetime(2026, 3, 1),
        )
        sql, params = _json_explain_sql(dialect, query)
        rows = pg_partition_explain_backend.fetch_all(sql, params)
        result = PostgresExplainResult(raw_rows=rows, sql=sql, duration=0.0)
        names = _extract_relation_names(result)

        expected = PARTITION_NAMES["p2026_02"][0]
        assert expected in names, f"Expected partition {expected} in query plan, got {names}"

    def test_explain_json_full_scan_lists_scan_method(
        self,
        pg_partition_explain_backend,
    ):
        """Full scan on partitioned table should show an Append or Seq Scan plan."""
        dialect = pg_partition_explain_backend.dialect
        sql, params = _json_explain_sql(dialect, _full_scan_query_expression(dialect))
        rows = pg_partition_explain_backend.fetch_all(sql, params)
        result = PostgresExplainResult(raw_rows=rows, sql=sql, duration=0.0)
        plan_text = str(result.raw_rows)
        assert "Append" in plan_text or "Seq Scan" in plan_text

    def test_explain_with_verbose_contains_partition_info(
        self,
        pg_partition_explain_backend,
    ):
        """EXPLAIN VERBOSE should expose partition-related details."""
        dialect = pg_partition_explain_backend.dialect
        query = _range_query_expression(
            dialect,
            datetime(2026, 1, 1),
            datetime(2026, 2, 1),
        )
        verbose_expr = ExplainExpression(
            dialect,
            query,
            options=ExplainOptions(verbose=True),
        )
        sql, params = verbose_expr.to_sql()
        rows = pg_partition_explain_backend.fetch_all(sql, params)
        result = PostgresExplainResult(raw_rows=rows, sql=sql, duration=0.0)
        plan = _combined_plan_text(result)
        expected = PARTITION_NAMES["p2026_01"][0]
        assert expected in plan, f"Expected partition {expected} in verbose plan, got {plan}"

    def test_explain_full_scan_returns_rows(
        self,
        pg_partition_explain_backend,
    ):
        """Basic EXPLAIN on full scan should return a non-empty result."""
        dialect = pg_partition_explain_backend.dialect
        result = pg_partition_explain_backend.explain(
            _full_scan_query_expression(dialect)
        )
        assert isinstance(result, PostgresExplainResult)
        assert len(result.raw_rows) > 0


class TestAsyncPostgresPartitionExplain:
    """Asynchronous EXPLAIN tests for PostgreSQL partition pruning."""

    @pytest.mark.asyncio
    async def test_explain_partition_pruning_on_range_predicate(
        self,
        async_pg_partition_explain_backend,
    ):
        """Range predicate on partition key should prune unrelated partitions."""
        dialect = async_pg_partition_explain_backend.dialect
        query = _range_query_expression(
            dialect,
            datetime(2026, 2, 1),
            datetime(2026, 3, 1),
        )
        result = await async_pg_partition_explain_backend.explain(query)
        assert isinstance(result, PostgresExplainResult)
        assert len(result.raw_rows) > 0

    @pytest.mark.asyncio
    async def test_explain_json_reports_target_partition(
        self,
        async_pg_partition_explain_backend,
    ):
        """EXPLAIN FORMAT JSON should expose the pruned target partition name."""
        dialect = async_pg_partition_explain_backend.dialect
        query = _range_query_expression(
            dialect,
            datetime(2026, 2, 1),
            datetime(2026, 3, 1),
        )
        sql, params = _json_explain_sql(dialect, query)
        rows = await async_pg_partition_explain_backend.fetch_all(sql, params)
        result = PostgresExplainResult(raw_rows=rows, sql=sql, duration=0.0)
        names = _extract_relation_names(result)
        expected = PARTITION_NAMES["p2026_02"][0]
        assert expected in names, f"Expected partition {expected} in query plan, got {names}"

    @pytest.mark.asyncio
    async def test_explain_with_verbose_contains_partition_info(
        self,
        async_pg_partition_explain_backend,
    ):
        """EXPLAIN VERBOSE should expose partition-related details."""
        dialect = async_pg_partition_explain_backend.dialect
        query = _range_query_expression(
            dialect,
            datetime(2026, 1, 1),
            datetime(2026, 2, 1),
        )
        verbose_expr = ExplainExpression(
            dialect,
            query,
            options=ExplainOptions(verbose=True),
        )
        sql, params = verbose_expr.to_sql()
        rows = await async_pg_partition_explain_backend.fetch_all(sql, params)
        result = PostgresExplainResult(raw_rows=rows, sql=sql, duration=0.0)
        plan = _combined_plan_text(result)
        expected = PARTITION_NAMES["p2026_01"][0]
        assert expected in plan, f"Expected partition {expected} in verbose plan, got {plan}"

    @pytest.mark.asyncio
    async def test_explain_full_scan_returns_rows(
        self,
        async_pg_partition_explain_backend,
    ):
        """Basic EXPLAIN on full scan should return a non-empty result."""
        dialect = async_pg_partition_explain_backend.dialect
        result = await async_pg_partition_explain_backend.explain(
            _full_scan_query_expression(dialect)
        )
        assert isinstance(result, PostgresExplainResult)
        assert len(result.raw_rows) > 0
