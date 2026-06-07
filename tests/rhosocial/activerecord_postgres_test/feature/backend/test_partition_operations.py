# tests/rhosocial/activerecord_postgres_test/feature/backend/test_partition_operations.py
"""Real PostgreSQL partition operation tests.

These tests execute against the configured PostgreSQL scenarios. They cover
common operational needs for declarative partitioning and keep synchronous and
asynchronous test method names identical across test classes.
"""
from datetime import datetime
from typing import Optional, Sequence

import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.expression import (
    Column,
    CreateIndexExpression,
    DeleteExpression,
    FunctionCall,
    InsertExpression,
    Literal,
    LogicalPredicate,
    OrderByClause,
    QualifiedIdentifierExpression,
    QueryExpression,
    TableExpression,
    ValuesSource,
    WildcardExpression,
)
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    CreateTableExpression,
    DropTableExpression,
    PartitionClause,
    PartitionStrategy,
    TableConstraint,
    TableConstraintType,
    TruncateExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression import (
    PostgresAttachPartitionExpression,
    PostgresCreatePartitionExpression,
    PostgresDetachPartitionExpression,
    PostgresPartitionMetadataExpression,
    PostgresPgPartmanCreateParentExpression,
    PostgresPgPartmanDeleteConfigExpression,
    PostgresPgPartmanRunMaintenanceExpression,
    PostgresPgPartmanUpdateConfigExpression,
)
from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType
from rhosocial.activerecord_postgres_test.feature.backend.utils import (
    ensure_extension_installed,
    async_ensure_extension_installed,
)


PARTITION_TABLES = (
    "ar_partition_events_p2026_01",
    "ar_partition_events_p2026_02",
    "ar_partition_events_p2026_03",
    "ar_partition_events_default",
    "ar_partition_events_archive",
    "ar_partition_events",
)

PARTMAN_TABLES = (
    "ar_partman_events",
)


def _qualified(table_name: str) -> str:
    return f"public.{table_name}"


def _create_partitioned_parent_sql(dialect, table_name: str):
    expr = CreateTableExpression(
        dialect=dialect,
        table=table_name,
        columns=[
            ColumnDefinition("id", "BIGINT NOT NULL"),
            ColumnDefinition("created_at", "TIMESTAMP NOT NULL"),
            ColumnDefinition("payload", "TEXT"),
        ],
        partition=PartitionClause(
            dialect=dialect,
            method=PartitionStrategy.RANGE,
            keys=[Column(dialect, "created_at")],
        ),
    )
    return expr.to_sql()


def _create_range_partition_sql(dialect, partition_name: str, from_value: str, to_value: str):
    expr = PostgresCreatePartitionExpression(
        dialect=dialect,
        partition_name=partition_name,
        parent_table="ar_partition_events",
        partition_type="RANGE",
        partition_values={"from": from_value, "to": to_value},
    )
    return expr.to_sql()


def _create_default_partition_sql(dialect, partition_name: str):
    expr = PostgresCreatePartitionExpression(
        dialect=dialect,
        partition_name=partition_name,
        parent_table="ar_partition_events",
        partition_type="RANGE",
        partition_values={"default": True},
    )
    return expr.to_sql()


def _delete_partition_events_for_range_expression(dialect, start, end):
    return DeleteExpression(
        dialect=dialect,
        tables="ar_partition_events",
        where=(Column(dialect, "created_at") >= Literal(dialect, start))
        & (Column(dialect, "created_at") < Literal(dialect, end)),
    )


def _insert_partition_events_expression(dialect, rows):
    return InsertExpression(
        dialect=dialect,
        into="ar_partition_events",
        columns=["id", "created_at", "payload"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, value) for value in row] for row in rows],
        ),
    )


def _detach_partition_sql(dialect, partition_name: str):
    expr = PostgresDetachPartitionExpression(
        dialect=dialect,
        partition_name=partition_name,
        parent_table="ar_partition_events",
    )
    return expr.to_sql()


def _attach_partition_sql(dialect, partition_name: str, from_value: str, to_value: str):
    expr = PostgresAttachPartitionExpression(
        dialect=dialect,
        partition_name=partition_name,
        parent_table="ar_partition_events",
        partition_type="RANGE",
        partition_values={"from": from_value, "to": to_value},
    )
    return expr.to_sql()


def _drop_table_sql(dialect, table_name: str):
    expr = DropTableExpression(
        dialect=dialect,
        table=table_name,
        if_exists=True,
        cascade=True,
    )
    return expr.to_sql()


def _truncate_table_sql(dialect, table_name: str):
    expr = TruncateExpression(
        dialect=dialect,
        table_name=table_name,
    )
    return expr.to_sql()


def _partition_metadata_sql(dialect, table_name: str):
    expr = PostgresPartitionMetadataExpression(
        dialect=dialect,
        parent_table=table_name,
    )
    return expr.to_sql()


PRODUCTION_PARTITION_TABLE = "ar_partition_ops_events"
PRODUCTION_DETACHED_TABLE = "ar_partition_ops_events_p2026"
PRODUCTION_PARTITION_TABLES = (
    "ar_partition_ops_events_p2026",
    "ar_partition_ops_events_p2027_q1",
    "ar_partition_ops_events_p2027_04",
    "ar_partition_ops_events_p2027_w18",
    "ar_partition_ops_events",
)
PRODUCTION_PARTITIONS = (
    ("ar_partition_ops_events_p2026", "2026-01-01 00:00:00.000000", "2027-01-01 00:00:00.000000"),
    ("ar_partition_ops_events_p2027_q1", "2027-01-01 00:00:00.000000", "2027-04-01 00:00:00.000000"),
    ("ar_partition_ops_events_p2027_04", "2027-04-01 00:00:00.000000", "2027-05-01 00:00:00.000000"),
    ("ar_partition_ops_events_p2027_w18", "2027-05-01 00:00:00.000000", "2027-05-08 00:00:00.000000"),
)


def _create_production_parent_sql(dialect):
    expr = CreateTableExpression(
        dialect=dialect,
        table=PRODUCTION_PARTITION_TABLE,
        columns=[
            ColumnDefinition("id", "BIGINT NOT NULL"),
            ColumnDefinition("created_at", "TIMESTAMP(6) NOT NULL"),
            ColumnDefinition("tenant_id", "BIGINT NOT NULL"),
            ColumnDefinition("payload", "TEXT NOT NULL"),
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
    return expr.to_sql()


def _create_production_partition_sql(dialect, partition_name: str, from_value: str, to_value: str):
    expr = PostgresCreatePartitionExpression(
        dialect=dialect,
        partition_name=partition_name,
        parent_table=PRODUCTION_PARTITION_TABLE,
        partition_type="RANGE",
        partition_values={"from": from_value, "to": to_value},
    )
    return expr.to_sql()


def _detach_production_partition_sql(dialect, partition_name: str):
    expr = PostgresDetachPartitionExpression(
        dialect=dialect,
        partition_name=partition_name,
        parent_table=PRODUCTION_PARTITION_TABLE,
    )
    return expr.to_sql()


def _attach_production_partition_sql(dialect, partition_name: str, from_value: str, to_value: str):
    expr = PostgresAttachPartitionExpression(
        dialect=dialect,
        partition_name=partition_name,
        parent_table=PRODUCTION_PARTITION_TABLE,
        partition_type="RANGE",
        partition_values={"from": from_value, "to": to_value},
    )
    return expr.to_sql()


def _create_production_index_sql(dialect, table_name: str):
    expr = CreateIndexExpression(
        dialect,
        f"idx_{table_name}_tenant_created_at",
        table_name,
        ["tenant_id", "created_at"],
        if_not_exists=True,
    )
    return expr.to_sql()


def _insert_production_events_expression(dialect, rows):
    return InsertExpression(
        dialect=dialect,
        into=PRODUCTION_PARTITION_TABLE,
        columns=["id", "created_at", "tenant_id", "payload"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, value) for value in row] for row in rows],
        ),
    )


def _select_production_payloads_expression(dialect, start, end, *, tenant_id: Optional[int] = None):
    predicate = (Column(dialect, "created_at") >= Literal(dialect, start)) & (
        Column(dialect, "created_at") < Literal(dialect, end)
    )
    if tenant_id is not None:
        predicate = LogicalPredicate(
            dialect,
            "AND",
            Column(dialect, "tenant_id") == Literal(dialect, tenant_id),
            predicate,
        )
    return QueryExpression(
        dialect,
        select=[Column(dialect, "payload")],
        from_=TableExpression(dialect, PRODUCTION_PARTITION_TABLE),
        where=predicate,
        order_by=OrderByClause(dialect, [(Column(dialect, "id"), "ASC")]),
    )


def _production_range_query_expression(dialect, start, end, *, tenant_id: int):
    return QueryExpression(
        dialect,
        select=[WildcardExpression(dialect)],
        from_=TableExpression(dialect, PRODUCTION_PARTITION_TABLE),
        where=LogicalPredicate(
            dialect,
            "AND",
            Column(dialect, "tenant_id") == Literal(dialect, tenant_id),
            (Column(dialect, "created_at") >= Literal(dialect, start))
            & (Column(dialect, "created_at") < Literal(dialect, end)),
        ),
    )


def _select_count_expression(dialect, table_name: str):
    return QueryExpression(
        dialect,
        select=[FunctionCall(dialect, "COUNT", WildcardExpression(dialect)).as_("count")],
        from_=TableExpression(dialect, table_name),
    )


def _production_partition_metadata_sql(dialect):
    expr = PostgresPartitionMetadataExpression(
        dialect=dialect,
        parent_table=PRODUCTION_PARTITION_TABLE,
    )
    return expr.to_sql()


def _production_partition_metadata(backend):
    return backend.fetch_all(*_production_partition_metadata_sql(backend.dialect))


async def _async_production_partition_metadata(backend):
    return await backend.fetch_all(*_production_partition_metadata_sql(backend.dialect))


def _assert_production_metadata(rows, expected_names: Sequence[str]):
    assert {row["name"] for row in rows} == set(expected_names)
    assert rows[0]["partition_key"] == "RANGE (created_at)"
    by_name = {row["name"]: row for row in rows}
    for name in expected_names:
        assert by_name[name]["bound"]


def _combined_plan(result) -> str:
    return "\n".join(row.line for row in result.rows).upper()


def _create_production_partitioned_table(backend, partitions):
    dialect = backend.dialect
    if not dialect.supports_partitioned_table_creation():
        pytest.skip("PostgreSQL scenario does not support declarative partitioning")
    if backend.get_server_version() < (11, 0, 0):
        pytest.skip("production partition primary key scenario requires PostgreSQL 11+")
    for table_name in PRODUCTION_PARTITION_TABLES:
        backend.execute(*_drop_table_sql(dialect, table_name))
    backend.execute(*_create_production_parent_sql(dialect))
    for partition_name, from_value, to_value in partitions:
        backend.execute(
            *_create_production_partition_sql(
                dialect,
                partition_name,
                from_value,
                to_value,
            )
        )
    for table_name, _, _ in partitions:
        backend.execute(*_create_production_index_sql(dialect, table_name))


async def _async_create_production_partitioned_table(backend, partitions):
    dialect = backend.dialect
    if not dialect.supports_partitioned_table_creation():
        pytest.skip("PostgreSQL scenario does not support declarative partitioning")
    if await backend.get_server_version() < (11, 0, 0):
        pytest.skip("production partition primary key scenario requires PostgreSQL 11+")
    for table_name in PRODUCTION_PARTITION_TABLES:
        await backend.execute(*_drop_table_sql(dialect, table_name))
    await backend.execute(*_create_production_parent_sql(dialect))
    for partition_name, from_value, to_value in partitions:
        await backend.execute(
            *_create_production_partition_sql(
                dialect,
                partition_name,
                from_value,
                to_value,
            )
        )
    for table_name, _, _ in partitions:
        await backend.execute(*_create_production_index_sql(dialect, table_name))


def _drop_production_partitioned_table(backend):
    for table_name in PRODUCTION_PARTITION_TABLES:
        backend.execute(*_drop_table_sql(backend.dialect, table_name))


async def _async_drop_production_partitioned_table(backend):
    for table_name in PRODUCTION_PARTITION_TABLES:
        await backend.execute(*_drop_table_sql(backend.dialect, table_name))


def _pg_partman_schema(backend) -> str:
    row = backend.fetch_one(
        """
        SELECT n.nspname AS schema_name
        FROM pg_extension e
        JOIN pg_namespace n ON n.oid = e.extnamespace
        WHERE e.extname = %s
        """,
        ("pg_partman",),
    )
    if row is None:
        pytest.skip("pg_partman extension is not installed")
    return row["schema_name"]


async def _async_pg_partman_schema(backend) -> str:
    row = await backend.fetch_one(
        """
        SELECT n.nspname AS schema_name
        FROM pg_extension e
        JOIN pg_namespace n ON n.oid = e.extnamespace
        WHERE e.extname = %s
        """,
        ("pg_partman",),
    )
    if row is None:
        pytest.skip("pg_partman extension is not installed")
    return row["schema_name"]


def _pg_partman_create_parent_sql(dialect, partman_schema: str):
    expr = PostgresPgPartmanCreateParentExpression(
        dialect=dialect,
        parent_table=_qualified("ar_partman_events"),
        control="created_at",
        interval="1 month",
        partition_type="range",
        premake=1,
        schema=partman_schema,
    )
    return expr.to_sql()


def _pg_partman_run_maintenance_sql(dialect, partman_schema: str):
    expr = PostgresPgPartmanRunMaintenanceExpression(
        dialect=dialect,
        parent_table=_qualified("ar_partman_events"),
        schema=partman_schema,
    )
    return expr.to_sql()


def _pg_partman_global_run_maintenance_sql(dialect, partman_schema: str):
    expr = PostgresPgPartmanRunMaintenanceExpression(
        dialect=dialect,
        schema=partman_schema,
    )
    return expr.to_sql()


def _pg_partman_update_config_sql(dialect, partman_schema: str):
    expr = PostgresPgPartmanUpdateConfigExpression(
        dialect=dialect,
        parent_table=_qualified("ar_partman_events"),
        automatic_maintenance="on",
        infinite_time_partitions=True,
        schema=partman_schema,
    )
    return expr.to_sql()


def _pg_partman_delete_config_sql(dialect, partman_schema: str):
    expr = PostgresPgPartmanDeleteConfigExpression(
        dialect=dialect,
        parent_table=_qualified("ar_partman_events"),
        schema=partman_schema,
    )
    return expr.to_sql()


def _pg_partman_config_table_sql(dialect, partman_schema: str) -> str:
    sql, _ = QualifiedIdentifierExpression(
        dialect=dialect,
        schema=partman_schema,
        name="part_config",
    ).to_sql()
    return sql


@pytest.fixture
def partitioned_event_table(postgres_backend):
    """Create a range-partitioned event table with two initial partitions."""
    dialect = postgres_backend.dialect
    if not dialect.supports_partitioned_table_creation():
        pytest.skip("PostgreSQL scenario does not support declarative partitioning")

    for table_name in PARTITION_TABLES:
        sql, params = _drop_table_sql(dialect, table_name)
        postgres_backend.execute(sql, params)

    sql, params = _create_partitioned_parent_sql(dialect, "ar_partition_events")
    postgres_backend.execute(sql, params)

    for partition_name, from_value, to_value in (
        ("ar_partition_events_p2026_01", "2026-01-01", "2026-02-01"),
        ("ar_partition_events_p2026_02", "2026-02-01", "2026-03-01"),
    ):
        sql, params = _create_range_partition_sql(dialect, partition_name, from_value, to_value)
        postgres_backend.execute(sql, params)

    yield "ar_partition_events"

    for table_name in PARTITION_TABLES:
        sql, params = _drop_table_sql(dialect, table_name)
        postgres_backend.execute(sql, params)


@pytest_asyncio.fixture
async def async_partitioned_event_table(async_postgres_backend):
    """Async counterpart for range-partitioned event table setup."""
    dialect = async_postgres_backend.dialect
    if not dialect.supports_partitioned_table_creation():
        pytest.skip("PostgreSQL scenario does not support declarative partitioning")

    for table_name in PARTITION_TABLES:
        sql, params = _drop_table_sql(dialect, table_name)
        await async_postgres_backend.execute(sql, params)

    sql, params = _create_partitioned_parent_sql(dialect, "ar_partition_events")
    await async_postgres_backend.execute(sql, params)

    for partition_name, from_value, to_value in (
        ("ar_partition_events_p2026_01", "2026-01-01", "2026-02-01"),
        ("ar_partition_events_p2026_02", "2026-02-01", "2026-03-01"),
    ):
        sql, params = _create_range_partition_sql(dialect, partition_name, from_value, to_value)
        await async_postgres_backend.execute(sql, params)

    yield "ar_partition_events"

    for table_name in PARTITION_TABLES:
        sql, params = _drop_table_sql(dialect, table_name)
        await async_postgres_backend.execute(sql, params)


@pytest.fixture
def postgres_production_year_partition_table(postgres_backend):
    """Create a production-like table with only the 2026 year partition."""
    _create_production_partitioned_table(postgres_backend, [PRODUCTION_PARTITIONS[0]])
    yield PRODUCTION_PARTITION_TABLE
    _drop_production_partitioned_table(postgres_backend)


@pytest_asyncio.fixture
async def async_postgres_production_year_partition_table(async_postgres_backend):
    """Create the async production-like table with only the 2026 partition."""
    await _async_create_production_partitioned_table(
        async_postgres_backend,
        [PRODUCTION_PARTITIONS[0]],
    )
    yield PRODUCTION_PARTITION_TABLE
    await _async_drop_production_partitioned_table(async_postgres_backend)


@pytest.fixture
def postgres_production_partition_table(postgres_backend):
    """Create a production-like table with year, quarter, month, and week partitions."""
    _create_production_partitioned_table(postgres_backend, PRODUCTION_PARTITIONS)
    yield PRODUCTION_PARTITION_TABLE
    _drop_production_partitioned_table(postgres_backend)


@pytest_asyncio.fixture
async def async_postgres_production_partition_table(async_postgres_backend):
    """Create the async production-like table with multiple future partitions."""
    await _async_create_production_partitioned_table(
        async_postgres_backend,
        PRODUCTION_PARTITIONS,
    )
    yield PRODUCTION_PARTITION_TABLE
    await _async_drop_production_partitioned_table(async_postgres_backend)


@pytest.fixture
def pg_partman_table(postgres_backend_single):
    """Create an isolated table for pg_partman maintenance tests."""
    ensure_extension_installed(postgres_backend_single, "pg_partman")
    dialect = postgres_backend_single.dialect

    partman_schema = _pg_partman_schema(postgres_backend_single)
    sql, params = _pg_partman_delete_config_sql(dialect, partman_schema)
    postgres_backend_single.execute(sql, params)
    for table_name in PARTMAN_TABLES:
        sql, params = _drop_table_sql(dialect, table_name)
        postgres_backend_single.execute(sql, params)

    sql, params = _create_partitioned_parent_sql(dialect, "ar_partman_events")
    postgres_backend_single.execute(sql, params)

    yield "ar_partman_events"

    sql, params = _pg_partman_delete_config_sql(dialect, partman_schema)
    postgres_backend_single.execute(sql, params)
    for table_name in PARTMAN_TABLES:
        sql, params = _drop_table_sql(dialect, table_name)
        postgres_backend_single.execute(sql, params)


@pytest_asyncio.fixture
async def async_pg_partman_table(async_postgres_backend_single):
    """Async counterpart for pg_partman table setup."""
    await async_ensure_extension_installed(async_postgres_backend_single, "pg_partman")
    dialect = async_postgres_backend_single.dialect

    partman_schema = await _async_pg_partman_schema(async_postgres_backend_single)
    sql, params = _pg_partman_delete_config_sql(dialect, partman_schema)
    await async_postgres_backend_single.execute(sql, params)
    for table_name in PARTMAN_TABLES:
        sql, params = _drop_table_sql(dialect, table_name)
        await async_postgres_backend_single.execute(sql, params)

    sql, params = _create_partitioned_parent_sql(dialect, "ar_partman_events")
    await async_postgres_backend_single.execute(sql, params)

    yield "ar_partman_events"

    sql, params = _pg_partman_delete_config_sql(dialect, partman_schema)
    await async_postgres_backend_single.execute(sql, params)
    for table_name in PARTMAN_TABLES:
        sql, params = _drop_table_sql(dialect, table_name)
        await async_postgres_backend_single.execute(sql, params)


class TestPostgreSQLPartitionOperations:
    """Synchronous real backend tests for common partition operations."""

    def test_partition_routing_and_pruning_metadata(self, postgres_backend, partitioned_event_table):
        """Rows route to expected partitions and pg metadata records the parent."""
        postgres_backend.execute(
            """
            INSERT INTO ar_partition_events (id, created_at, payload)
            VALUES (%s, %s, %s), (%s, %s, %s)
            """,
            (1, "2026-01-15", "jan", 2, "2026-02-15", "feb"),
        )

        rows = postgres_backend.fetch_all(
            """
            SELECT tableoid::regclass::text AS partition_name, payload
            FROM ar_partition_events
            ORDER BY id
            """
        )
        assert [row["partition_name"] for row in rows] == [
            "ar_partition_events_p2026_01",
            "ar_partition_events_p2026_02",
        ]

        sql, params = _partition_metadata_sql(postgres_backend.dialect, partitioned_event_table)
        metadata = postgres_backend.fetch_all(sql, params)
        assert metadata[0]["partition_key"] == "RANGE (created_at)"
        assert {row["name"] for row in metadata} == {
            "ar_partition_events_p2026_01",
            "ar_partition_events_p2026_02",
        }

    def test_add_partition_for_future_range(self, postgres_backend, partitioned_event_table):
        """A future range partition can be added for normal operations."""
        dialect = postgres_backend.dialect
        sql, params = _create_range_partition_sql(
            dialect,
            "ar_partition_events_p2026_03",
            "2026-03-01",
            "2026-04-01",
        )
        postgres_backend.execute(sql, params)
        postgres_backend.execute(
            *_insert_partition_events_expression(
                dialect,
                [(3, "2026-03-15", "mar")],
            ).to_sql()
        )

        row = postgres_backend.fetch_one(
            """
            SELECT tableoid::regclass::text AS partition_name
            FROM ar_partition_events
            WHERE id = %s
            """,
            (3,),
        )
        assert row["partition_name"] == "ar_partition_events_p2026_03"

    def test_default_partition_catches_overflow_and_blocks_conflicting_range(
        self,
        postgres_backend,
        partitioned_event_table,
    ):
        """DEFAULT partition catches overflow until operators split a concrete range.

                Scenario: a DEFAULT partition keeps out-of-range events writable while
                operators prepare the next explicit range partition.

                Steps: create a DEFAULT partition, insert an April row into it, prove a
                conflicting April partition cannot be added, delete the conflicting row,
                then add the April range partition and insert a replacement row.

                Assertions: overflow rows route to the DEFAULT partition; PostgreSQL
                rejects a concrete partition whose range overlaps data already stored in
                DEFAULT; after cleanup, the new concrete range accepts the replacement row.

                Production value: this captures the default-partition maintenance runbook
                and proves that cleanup is required before splitting catch-all data.
        """
        dialect = postgres_backend.dialect
        if not dialect.supports_default_partition():
            pytest.skip("PostgreSQL scenario does not support DEFAULT partitions")
        postgres_backend.execute(
            *_create_default_partition_sql(dialect, "ar_partition_events_default")
        )
        postgres_backend.execute(
            *_insert_partition_events_expression(
                dialect,
                [(30, "2026-04-15", "default-overflow")],
            ).to_sql()
        )

        row = postgres_backend.fetch_one(
            """
            SELECT tableoid::regclass::text AS partition_name, payload
            FROM ar_partition_events
            WHERE id = %s
            """,
            (30,),
        )
        assert row["partition_name"] == "ar_partition_events_default"
        assert row["payload"] == "default-overflow"

        with pytest.raises(Exception):
            postgres_backend.execute(
                *_create_range_partition_sql(
                    dialect,
                    "ar_partition_events_p2026_03",
                    "2026-04-01",
                    "2026-05-01",
                )
            )
        postgres_backend.execute(
            *_delete_partition_events_for_range_expression(
                dialect,
                "2026-04-01",
                "2026-05-01",
            ).to_sql()
        )
        postgres_backend.execute(
            *_create_range_partition_sql(
                dialect,
                "ar_partition_events_p2026_03",
                "2026-04-01",
                "2026-05-01",
            )
        )
        postgres_backend.execute(
            *_insert_partition_events_expression(
                dialect,
                [(31, "2026-04-16", "split-range")],
            ).to_sql()
        )

        row = postgres_backend.fetch_one(
            """
            SELECT tableoid::regclass::text AS partition_name, payload
            FROM ar_partition_events
            WHERE id = %s
            """,
            (31,),
        )
        assert row["partition_name"] == "ar_partition_events_p2026_03"
        assert row["payload"] == "split-range"

    def test_detach_partition_for_archive_and_reattach(self, postgres_backend, partitioned_event_table):
        """A partition can be detached for archive work and attached back."""
        dialect = postgres_backend.dialect
        postgres_backend.execute(
            "INSERT INTO ar_partition_events (id, created_at, payload) VALUES (%s, %s, %s)",
            (1, "2026-01-15", "jan"),
        )

        sql, params = _detach_partition_sql(dialect, "ar_partition_events_p2026_01")
        postgres_backend.execute(sql, params)

        parent_count = postgres_backend.fetch_one(
            "SELECT COUNT(*) AS count FROM ar_partition_events"
        )["count"]
        archive_count = postgres_backend.fetch_one(
            "SELECT COUNT(*) AS count FROM ar_partition_events_p2026_01"
        )["count"]
        assert parent_count == 0
        assert archive_count == 1

        sql, params = _attach_partition_sql(
            dialect,
            "ar_partition_events_p2026_01",
            "2026-01-01",
            "2026-02-01",
        )
        postgres_backend.execute(sql, params)

        parent_count = postgres_backend.fetch_one(
            "SELECT COUNT(*) AS count FROM ar_partition_events"
        )["count"]
        assert parent_count == 1

    def test_truncate_partition_keeps_partition_available(self, postgres_backend, partitioned_event_table):
        """A single partition can be truncated without dropping the parent."""
        postgres_backend.execute(
            "INSERT INTO ar_partition_events (id, created_at, payload) VALUES (%s, %s, %s)",
            (1, "2026-01-15", "jan"),
        )
        sql, params = _truncate_table_sql(postgres_backend.dialect, "ar_partition_events_p2026_01")
        postgres_backend.execute(sql, params)

        count = postgres_backend.fetch_one(
            "SELECT COUNT(*) AS count FROM ar_partition_events"
        )["count"]
        assert count == 0

        postgres_backend.execute(
            "INSERT INTO ar_partition_events (id, created_at, payload) VALUES (%s, %s, %s)",
            (2, "2026-01-20", "jan-again"),
        )
        count = postgres_backend.fetch_one(
            "SELECT COUNT(*) AS count FROM ar_partition_events"
        )["count"]
        assert count == 1


class TestAsyncPostgreSQLPartitionOperations:
    """Asynchronous real backend tests for common partition operations."""

    @pytest.mark.asyncio
    async def test_partition_routing_and_pruning_metadata(
        self,
        async_postgres_backend,
        async_partitioned_event_table,
    ):
        """Rows route to expected partitions and pg metadata records the parent."""
        await async_postgres_backend.execute(
            """
            INSERT INTO ar_partition_events (id, created_at, payload)
            VALUES (%s, %s, %s), (%s, %s, %s)
            """,
            (1, "2026-01-15", "jan", 2, "2026-02-15", "feb"),
        )

        rows = await async_postgres_backend.fetch_all(
            """
            SELECT tableoid::regclass::text AS partition_name, payload
            FROM ar_partition_events
            ORDER BY id
            """
        )
        assert [row["partition_name"] for row in rows] == [
            "ar_partition_events_p2026_01",
            "ar_partition_events_p2026_02",
        ]

        sql, params = _partition_metadata_sql(
            async_postgres_backend.dialect,
            async_partitioned_event_table,
        )
        metadata = await async_postgres_backend.fetch_all(sql, params)
        assert metadata[0]["partition_key"] == "RANGE (created_at)"
        assert {row["name"] for row in metadata} == {
            "ar_partition_events_p2026_01",
            "ar_partition_events_p2026_02",
        }

    @pytest.mark.asyncio
    async def test_add_partition_for_future_range(
        self,
        async_postgres_backend,
        async_partitioned_event_table,
    ):
        """A future range partition can be added for normal operations."""
        dialect = async_postgres_backend.dialect
        sql, params = _create_range_partition_sql(
            dialect,
            "ar_partition_events_p2026_03",
            "2026-03-01",
            "2026-04-01",
        )
        await async_postgres_backend.execute(sql, params)
        await async_postgres_backend.execute(
            *_insert_partition_events_expression(
                dialect,
                [(3, "2026-03-15", "mar")],
            ).to_sql()
        )

        row = await async_postgres_backend.fetch_one(
            """
            SELECT tableoid::regclass::text AS partition_name
            FROM ar_partition_events
            WHERE id = %s
            """,
            (3,),
        )
        assert row["partition_name"] == "ar_partition_events_p2026_03"

    @pytest.mark.asyncio
    async def test_default_partition_catches_overflow_and_blocks_conflicting_range(
        self,
        async_postgres_backend,
        async_partitioned_event_table,
    ):
        """DEFAULT partition catches overflow until operators split a concrete range.

                Scenario: a DEFAULT partition keeps out-of-range events writable while
                operators prepare the next explicit range partition.

                Steps: create a DEFAULT partition, insert an April row into it, prove a
                conflicting April partition cannot be added, delete the conflicting row,
                then add the April range partition and insert a replacement row.

                Assertions: overflow rows route to the DEFAULT partition; PostgreSQL
                rejects a concrete partition whose range overlaps data already stored in
                DEFAULT; after cleanup, the new concrete range accepts the replacement row.

                Production value: this captures the default-partition maintenance runbook
                and proves that cleanup is required before splitting catch-all data.
        """
        dialect = async_postgres_backend.dialect
        if not dialect.supports_default_partition():
            pytest.skip("PostgreSQL scenario does not support DEFAULT partitions")
        await async_postgres_backend.execute(
            *_create_default_partition_sql(dialect, "ar_partition_events_default")
        )
        await async_postgres_backend.execute(
            *_insert_partition_events_expression(
                dialect,
                [(30, "2026-04-15", "default-overflow")],
            ).to_sql()
        )

        row = await async_postgres_backend.fetch_one(
            """
            SELECT tableoid::regclass::text AS partition_name, payload
            FROM ar_partition_events
            WHERE id = %s
            """,
            (30,),
        )
        assert row["partition_name"] == "ar_partition_events_default"
        assert row["payload"] == "default-overflow"

        with pytest.raises(Exception):
            await async_postgres_backend.execute(
                *_create_range_partition_sql(
                    dialect,
                    "ar_partition_events_p2026_03",
                    "2026-04-01",
                    "2026-05-01",
                )
            )
        await async_postgres_backend.execute(
            *_delete_partition_events_for_range_expression(
                dialect,
                "2026-04-01",
                "2026-05-01",
            ).to_sql()
        )
        await async_postgres_backend.execute(
            *_create_range_partition_sql(
                dialect,
                "ar_partition_events_p2026_03",
                "2026-04-01",
                "2026-05-01",
            )
        )
        await async_postgres_backend.execute(
            *_insert_partition_events_expression(
                dialect,
                [(31, "2026-04-16", "split-range")],
            ).to_sql()
        )

        row = await async_postgres_backend.fetch_one(
            """
            SELECT tableoid::regclass::text AS partition_name, payload
            FROM ar_partition_events
            WHERE id = %s
            """,
            (31,),
        )
        assert row["partition_name"] == "ar_partition_events_p2026_03"
        assert row["payload"] == "split-range"

    @pytest.mark.asyncio
    async def test_detach_partition_for_archive_and_reattach(
        self,
        async_postgres_backend,
        async_partitioned_event_table,
    ):
        """A partition can be detached for archive work and attached back."""
        dialect = async_postgres_backend.dialect
        await async_postgres_backend.execute(
            "INSERT INTO ar_partition_events (id, created_at, payload) VALUES (%s, %s, %s)",
            (1, "2026-01-15", "jan"),
        )

        sql, params = _detach_partition_sql(dialect, "ar_partition_events_p2026_01")
        await async_postgres_backend.execute(sql, params)

        parent_count = (await async_postgres_backend.fetch_one(
            "SELECT COUNT(*) AS count FROM ar_partition_events"
        ))["count"]
        archive_count = (await async_postgres_backend.fetch_one(
            "SELECT COUNT(*) AS count FROM ar_partition_events_p2026_01"
        ))["count"]
        assert parent_count == 0
        assert archive_count == 1

        sql, params = _attach_partition_sql(
            dialect,
            "ar_partition_events_p2026_01",
            "2026-01-01",
            "2026-02-01",
        )
        await async_postgres_backend.execute(sql, params)

        parent_count = (await async_postgres_backend.fetch_one(
            "SELECT COUNT(*) AS count FROM ar_partition_events"
        ))["count"]
        assert parent_count == 1

    @pytest.mark.asyncio
    async def test_truncate_partition_keeps_partition_available(
        self,
        async_postgres_backend,
        async_partitioned_event_table,
    ):
        """A single partition can be truncated without dropping the parent."""
        await async_postgres_backend.execute(
            "INSERT INTO ar_partition_events (id, created_at, payload) VALUES (%s, %s, %s)",
            (1, "2026-01-15", "jan"),
        )
        sql, params = _truncate_table_sql(async_postgres_backend.dialect, "ar_partition_events_p2026_01")
        await async_postgres_backend.execute(sql, params)

        count = (await async_postgres_backend.fetch_one(
            "SELECT COUNT(*) AS count FROM ar_partition_events"
        ))["count"]
        assert count == 0

        await async_postgres_backend.execute(
            "INSERT INTO ar_partition_events (id, created_at, payload) VALUES (%s, %s, %s)",
            (2, "2026-01-20", "jan-again"),
        )
        count = (await async_postgres_backend.fetch_one(
            "SELECT COUNT(*) AS count FROM ar_partition_events"
        ))["count"]
        assert count == 1


class TestPostgreSQLProductionTimePartitionOperations:
    """Synchronous production-style time partition operation scenarios."""

    def test_initial_year_partition_uses_microsecond_boundaries(
        self,
        postgres_backend,
        postgres_production_year_partition_table,
    ):
        """Validate the initial annual partition used during table rollout.
        
                Scenario: the production table starts with only the 2026 calendar-year
                partition. The partition key is `created_at TIMESTAMP(6)`, and the
                primary key includes `(id, created_at)` to model a real time-partitioned
                table constraint.
        
                Steps: create the parent table with only the 2026 partition, insert the
                2026 lower bound and the last microsecond before 2027, then try to
                insert the first timestamp of 2027 before a future partition exists.
        
                Assertions: metadata lists only the annual partition; microsecond
                boundary rows are queryable through the parent table; out-of-range
                future data is rejected by PostgreSQL.
        
                Production value: this proves the annual rollout can cover a complete
                calendar year while exposing missing future partitions at the database
                boundary.
        """
        assert postgres_production_year_partition_table == PRODUCTION_PARTITION_TABLE
        _assert_production_metadata(
            _production_partition_metadata(postgres_backend),
            [PRODUCTION_DETACHED_TABLE],
        )
        postgres_backend.execute(
            *_insert_production_events_expression(
                postgres_backend.dialect,
                [
                    [1, datetime(2026, 1, 1, 0, 0, 0, 0), 10, "year-start"],
                    [2, datetime(2026, 12, 31, 23, 59, 59, 999999), 10, "year-end"],
                ],
            ).to_sql()
        )

        rows = postgres_backend.fetch_all(
            *_select_production_payloads_expression(
                postgres_backend.dialect,
                datetime(2026, 1, 1),
                datetime(2027, 1, 1),
            ).to_sql()
        )
        assert [row["payload"] for row in rows] == ["year-start", "year-end"]

        with pytest.raises(Exception):
            postgres_backend.execute(
                *_insert_production_events_expression(
                    postgres_backend.dialect,
                    [[3, datetime(2027, 1, 1, 0, 0, 0, 0), 10, "missing-partition"]],
                ).to_sql()
            )

    def test_precreate_future_partitions_before_traffic_arrives(
        self,
        postgres_backend,
        postgres_production_year_partition_table,
    ):
        """Pre-create future partitions with mixed operational granularities.
        
                Scenario: operators prepare future partitions before traffic arrives,
                and the future granularity may shift from yearly to quarterly, monthly,
                or weekly partitions.
        
                Steps: start from the initial 2026 annual partition, create the 2027 Q1,
                April 2027, and ISO-week-like 2027 week 18 partitions, then insert rows
                into each future window.
        
                Assertions: metadata reflects all newly created partitions; future rows
                are accepted; parent-table queries can read the full pre-created range.
        
                Production value: this verifies rolling partition pre-creation so
                traffic can cross time boundaries without emergency DDL.
        """
        for partition_name, from_value, to_value in PRODUCTION_PARTITIONS[1:]:
            postgres_backend.execute(
                *_create_production_partition_sql(
                    postgres_backend.dialect,
                    partition_name,
                    from_value,
                    to_value,
                )
            )

        _assert_production_metadata(
            _production_partition_metadata(postgres_backend),
            [name for name, _, _ in PRODUCTION_PARTITIONS],
        )
        postgres_backend.execute(
            *_insert_production_events_expression(
                postgres_backend.dialect,
                [
                    [11, datetime(2027, 2, 15, 8, 0, 0, 123456), 10, "q1"],
                    [12, datetime(2027, 4, 15, 8, 0, 0, 123456), 10, "month"],
                    [13, datetime(2027, 5, 3, 8, 0, 0, 123456), 10, "week"],
                ],
            ).to_sql()
        )

        rows = postgres_backend.fetch_all(
            *_select_production_payloads_expression(
                postgres_backend.dialect,
                datetime(2027, 1, 1),
                datetime(2027, 5, 8),
            ).to_sql()
        )
        assert [row["payload"] for row in rows] == ["q1", "month", "week"]

    def test_query_continuous_partitions_and_explain_uses_index(
        self,
        postgres_backend,
        postgres_production_partition_table,
    ):
        """Query a continuous time range and inspect pruning/index usage.
        
                Scenario: production searches often span several continuous partitions
                while filtering by tenant and time range, so both pruning and business
                indexes matter.
        
                Steps: insert rows across yearly, quarterly, monthly, and weekly
                partitions, query `[2027-02-01, 2027-05-08)` for `tenant_id=10`, and run
                EXPLAIN for the same QueryExpression.
        
                Assertions: the query returns only target-tenant rows in the continuous
                range; the plan includes target partitions and excludes the 2026 cold
                partition; disabling sequential scans exposes an index path.
        
                Production value: this proves PostgreSQL partition pruning does not
                replace business indexes, and continuous time-range searches should keep
                the `(tenant_id, created_at)` composite index.
        """
        postgres_backend.execute(
            *_insert_production_events_expression(
                postgres_backend.dialect,
                [
                    [21, datetime(2026, 6, 1), 10, "old-year"],
                    [22, datetime(2027, 2, 15), 10, "q1"],
                    [23, datetime(2027, 4, 15), 10, "month"],
                    [24, datetime(2027, 5, 3), 10, "week"],
                    [25, datetime(2027, 5, 3), 20, "other-tenant"],
                ],
            ).to_sql()
        )

        rows = postgres_backend.fetch_all(
            *_select_production_payloads_expression(
                postgres_backend.dialect,
                datetime(2027, 2, 1),
                datetime(2027, 5, 8),
                tenant_id=10,
            ).to_sql()
        )
        assert [row["payload"] for row in rows] == ["q1", "month", "week"]

        query = _production_range_query_expression(
            postgres_backend.dialect,
            datetime(2027, 2, 1),
            datetime(2027, 5, 8),
            tenant_id=10,
        )
        postgres_backend.execute("SET enable_seqscan = off")
        try:
            plan = _combined_plan(postgres_backend.explain(query))
        finally:
            postgres_backend.execute("RESET enable_seqscan")
        assert "AR_PARTITION_OPS_EVENTS_P2026" not in plan
        assert "AR_PARTITION_OPS_EVENTS_P2027_Q1" in plan
        assert "AR_PARTITION_OPS_EVENTS_P2027_04" in plan
        assert "AR_PARTITION_OPS_EVENTS_P2027_W18" in plan
        assert "INDEX" in plan

    def test_detach_expired_year_partition_for_cold_archive(
        self,
        postgres_backend,
        postgres_production_partition_table,
    ):
        """Detach an expired year partition for cold archival while keeping data.
        
                Scenario: when an expired annual partition becomes cold data,
                PostgreSQL can detach the child partition into a regular table without
                deleting rows.
        
                Steps: insert cold 2026 data and hot 2027 data, detach the 2026 annual
                partition, verify parent/detached-table visibility, then attach it back
                as a recovery path.
        
                Assertions: metadata no longer lists the 2026 partition after detach;
                the parent no longer returns cold data; the detached table keeps cold
                data; reattach makes cold data visible through the parent again.
        
                Production value: this documents the PostgreSQL cold-archive workflow
                for removing a partition from the hot parent without data loss.
        """
        postgres_backend.execute(
            *_insert_production_events_expression(
                postgres_backend.dialect,
                [
                    [31, datetime(2026, 6, 1), 10, "cold-year"],
                    [32, datetime(2027, 2, 1), 10, "hot-quarter"],
                ],
            ).to_sql()
        )
        postgres_backend.execute(
            *_detach_production_partition_sql(postgres_backend.dialect, PRODUCTION_DETACHED_TABLE)
        )

        metadata = _production_partition_metadata(postgres_backend)
        assert PRODUCTION_DETACHED_TABLE not in {row["name"] for row in metadata}
        parent_count = postgres_backend.fetch_one(
            *_select_count_expression(postgres_backend.dialect, PRODUCTION_PARTITION_TABLE).to_sql()
        )["count"]
        archive_count = postgres_backend.fetch_one(
            *_select_count_expression(postgres_backend.dialect, PRODUCTION_DETACHED_TABLE).to_sql()
        )["count"]
        assert parent_count == 1
        assert archive_count == 1

        postgres_backend.execute(
            *_attach_production_partition_sql(
                postgres_backend.dialect,
                PRODUCTION_DETACHED_TABLE,
                "2026-01-01 00:00:00.000000",
                "2027-01-01 00:00:00.000000",
            )
        )
        parent_count = postgres_backend.fetch_one(
            *_select_count_expression(postgres_backend.dialect, PRODUCTION_PARTITION_TABLE).to_sql()
        )["count"]
        assert parent_count == 2


class TestAsyncPostgreSQLProductionTimePartitionOperations:
    """Asynchronous production-style time partition operation scenarios."""

    @pytest.mark.asyncio
    async def test_initial_year_partition_uses_microsecond_boundaries(
        self,
        async_postgres_backend,
        async_postgres_production_year_partition_table,
    ):
        """Validate the initial annual partition used during table rollout.
        
                Scenario: the production table starts with only the 2026 calendar-year
                partition. The partition key is `created_at TIMESTAMP(6)`, and the
                primary key includes `(id, created_at)` to model a real time-partitioned
                table constraint.
        
                Steps: create the parent table with only the 2026 partition, insert the
                2026 lower bound and the last microsecond before 2027, then try to
                insert the first timestamp of 2027 before a future partition exists.
        
                Assertions: metadata lists only the annual partition; microsecond
                boundary rows are queryable through the parent table; out-of-range
                future data is rejected by PostgreSQL.
        
                Production value: this proves the annual rollout can cover a complete
                calendar year while exposing missing future partitions at the database
                boundary.
        """
        assert async_postgres_production_year_partition_table == PRODUCTION_PARTITION_TABLE
        rows = await _async_production_partition_metadata(async_postgres_backend)
        _assert_production_metadata(rows, [PRODUCTION_DETACHED_TABLE])
        await async_postgres_backend.execute(
            *_insert_production_events_expression(
                async_postgres_backend.dialect,
                [
                    [1, datetime(2026, 1, 1, 0, 0, 0, 0), 10, "year-start"],
                    [2, datetime(2026, 12, 31, 23, 59, 59, 999999), 10, "year-end"],
                ],
            ).to_sql()
        )

        rows = await async_postgres_backend.fetch_all(
            *_select_production_payloads_expression(
                async_postgres_backend.dialect,
                datetime(2026, 1, 1),
                datetime(2027, 1, 1),
            ).to_sql()
        )
        assert [row["payload"] for row in rows] == ["year-start", "year-end"]

        with pytest.raises(Exception):
            await async_postgres_backend.execute(
                *_insert_production_events_expression(
                    async_postgres_backend.dialect,
                    [[3, datetime(2027, 1, 1, 0, 0, 0, 0), 10, "missing-partition"]],
                ).to_sql()
            )

    @pytest.mark.asyncio
    async def test_precreate_future_partitions_before_traffic_arrives(
        self,
        async_postgres_backend,
        async_postgres_production_year_partition_table,
    ):
        """Pre-create future partitions with mixed operational granularities.
        
                Scenario: operators prepare future partitions before traffic arrives,
                and the future granularity may shift from yearly to quarterly, monthly,
                or weekly partitions.
        
                Steps: start from the initial 2026 annual partition, create the 2027 Q1,
                April 2027, and ISO-week-like 2027 week 18 partitions, then insert rows
                into each future window.
        
                Assertions: metadata reflects all newly created partitions; future rows
                are accepted; parent-table queries can read the full pre-created range.
        
                Production value: this verifies rolling partition pre-creation so
                traffic can cross time boundaries without emergency DDL.
        """
        for partition_name, from_value, to_value in PRODUCTION_PARTITIONS[1:]:
            await async_postgres_backend.execute(
                *_create_production_partition_sql(
                    async_postgres_backend.dialect,
                    partition_name,
                    from_value,
                    to_value,
                )
            )

        rows = await _async_production_partition_metadata(async_postgres_backend)
        _assert_production_metadata(rows, [name for name, _, _ in PRODUCTION_PARTITIONS])
        await async_postgres_backend.execute(
            *_insert_production_events_expression(
                async_postgres_backend.dialect,
                [
                    [11, datetime(2027, 2, 15, 8, 0, 0, 123456), 10, "q1"],
                    [12, datetime(2027, 4, 15, 8, 0, 0, 123456), 10, "month"],
                    [13, datetime(2027, 5, 3, 8, 0, 0, 123456), 10, "week"],
                ],
            ).to_sql()
        )

        rows = await async_postgres_backend.fetch_all(
            *_select_production_payloads_expression(
                async_postgres_backend.dialect,
                datetime(2027, 1, 1),
                datetime(2027, 5, 8),
            ).to_sql()
        )
        assert [row["payload"] for row in rows] == ["q1", "month", "week"]

    @pytest.mark.asyncio
    async def test_query_continuous_partitions_and_explain_uses_index(
        self,
        async_postgres_backend,
        async_postgres_production_partition_table,
    ):
        """Query a continuous time range and inspect pruning/index usage.
        
                Scenario: production searches often span several continuous partitions
                while filtering by tenant and time range, so both pruning and business
                indexes matter.
        
                Steps: insert rows across yearly, quarterly, monthly, and weekly
                partitions, query `[2027-02-01, 2027-05-08)` for `tenant_id=10`, and run
                EXPLAIN for the same QueryExpression.
        
                Assertions: the query returns only target-tenant rows in the continuous
                range; the plan includes target partitions and excludes the 2026 cold
                partition; disabling sequential scans exposes an index path.
        
                Production value: this proves PostgreSQL partition pruning does not
                replace business indexes, and continuous time-range searches should keep
                the `(tenant_id, created_at)` composite index.
        """
        await async_postgres_backend.execute(
            *_insert_production_events_expression(
                async_postgres_backend.dialect,
                [
                    [21, datetime(2026, 6, 1), 10, "old-year"],
                    [22, datetime(2027, 2, 15), 10, "q1"],
                    [23, datetime(2027, 4, 15), 10, "month"],
                    [24, datetime(2027, 5, 3), 10, "week"],
                    [25, datetime(2027, 5, 3), 20, "other-tenant"],
                ],
            ).to_sql()
        )

        rows = await async_postgres_backend.fetch_all(
            *_select_production_payloads_expression(
                async_postgres_backend.dialect,
                datetime(2027, 2, 1),
                datetime(2027, 5, 8),
                tenant_id=10,
            ).to_sql()
        )
        assert [row["payload"] for row in rows] == ["q1", "month", "week"]

        query = _production_range_query_expression(
            async_postgres_backend.dialect,
            datetime(2027, 2, 1),
            datetime(2027, 5, 8),
            tenant_id=10,
        )
        await async_postgres_backend.execute("SET enable_seqscan = off")
        try:
            plan = _combined_plan(await async_postgres_backend.explain(query))
        finally:
            await async_postgres_backend.execute("RESET enable_seqscan")
        assert "AR_PARTITION_OPS_EVENTS_P2026" not in plan
        assert "AR_PARTITION_OPS_EVENTS_P2027_Q1" in plan
        assert "AR_PARTITION_OPS_EVENTS_P2027_04" in plan
        assert "AR_PARTITION_OPS_EVENTS_P2027_W18" in plan
        assert "INDEX" in plan

    @pytest.mark.asyncio
    async def test_detach_expired_year_partition_for_cold_archive(
        self,
        async_postgres_backend,
        async_postgres_production_partition_table,
    ):
        """Detach an expired year partition for cold archival while keeping data.
        
                Scenario: when an expired annual partition becomes cold data,
                PostgreSQL can detach the child partition into a regular table without
                deleting rows.
        
                Steps: insert cold 2026 data and hot 2027 data, detach the 2026 annual
                partition, verify parent/detached-table visibility, then attach it back
                as a recovery path.
        
                Assertions: metadata no longer lists the 2026 partition after detach;
                the parent no longer returns cold data; the detached table keeps cold
                data; reattach makes cold data visible through the parent again.
        
                Production value: this documents the PostgreSQL cold-archive workflow
                for removing a partition from the hot parent without data loss.
        """
        await async_postgres_backend.execute(
            *_insert_production_events_expression(
                async_postgres_backend.dialect,
                [
                    [31, datetime(2026, 6, 1), 10, "cold-year"],
                    [32, datetime(2027, 2, 1), 10, "hot-quarter"],
                ],
            ).to_sql()
        )
        await async_postgres_backend.execute(
            *_detach_production_partition_sql(
                async_postgres_backend.dialect,
                PRODUCTION_DETACHED_TABLE,
            )
        )

        metadata = await _async_production_partition_metadata(async_postgres_backend)
        assert PRODUCTION_DETACHED_TABLE not in {row["name"] for row in metadata}
        parent_count = (await async_postgres_backend.fetch_one(
            *_select_count_expression(async_postgres_backend.dialect, PRODUCTION_PARTITION_TABLE).to_sql()
        ))["count"]
        archive_count = (await async_postgres_backend.fetch_one(
            *_select_count_expression(async_postgres_backend.dialect, PRODUCTION_DETACHED_TABLE).to_sql()
        ))["count"]
        assert parent_count == 1
        assert archive_count == 1

        await async_postgres_backend.execute(
            *_attach_production_partition_sql(
                async_postgres_backend.dialect,
                PRODUCTION_DETACHED_TABLE,
                "2026-01-01 00:00:00.000000",
                "2027-01-01 00:00:00.000000",
            )
        )
        parent_count = (await async_postgres_backend.fetch_one(
            *_select_count_expression(async_postgres_backend.dialect, PRODUCTION_PARTITION_TABLE).to_sql()
        ))["count"]
        assert parent_count == 2


class TestPostgreSQLPgPartmanOperations:
    """Synchronous pg_partman real backend tests."""

    def test_pg_partman_create_parent_and_run_maintenance(
        self,
        postgres_backend_single,
        pg_partman_table,
    ):
        """pg_partman can register a parent table, update config, and maintain it.

                Scenario: production automation delegates future partition maintenance to
                pg_partman while keeping configuration changes explicit.

                Steps: register the partitioned parent, update part_config options, run
                scoped maintenance, then run global maintenance.

                Assertions: part_config stores the parent and updated options; maintenance
                calls complete; partition metadata can be inspected after maintenance.

                Production value: this verifies the pg_partman runbook used by operators
                to keep future partitions maintained without manual DDL for every window.
        """
        partman_schema = _pg_partman_schema(postgres_backend_single)
        sql, params = _pg_partman_create_parent_sql(postgres_backend_single.dialect, partman_schema)
        options = ExecutionOptions(stmt_type=StatementType.DQL)
        postgres_backend_single.execute(sql, params, options=options)

        postgres_backend_single.execute(
            *_pg_partman_update_config_sql(postgres_backend_single.dialect, partman_schema)
        )

        config_table_sql = _pg_partman_config_table_sql(postgres_backend_single.dialect, partman_schema)
        row = postgres_backend_single.fetch_one(
            f"""
            SELECT parent_table, automatic_maintenance, infinite_time_partitions
            FROM {config_table_sql}
            WHERE parent_table = %s
            """,
            (_qualified(pg_partman_table),),
        )
        assert row is not None
        assert row["automatic_maintenance"] == "on"
        assert row["infinite_time_partitions"] is True

        sql, params = _pg_partman_run_maintenance_sql(postgres_backend_single.dialect, partman_schema)
        postgres_backend_single.execute(sql, params, options=options)
        sql, params = _pg_partman_global_run_maintenance_sql(
            postgres_backend_single.dialect,
            partman_schema,
        )
        postgres_backend_single.execute(sql, params, options=options)

        metadata_sql, metadata_params = _partition_metadata_sql(
            postgres_backend_single.dialect,
            pg_partman_table,
        )
        metadata = postgres_backend_single.fetch_all(metadata_sql, metadata_params)
        assert isinstance(metadata, list)


class TestAsyncPostgreSQLPgPartmanOperations:
    """Asynchronous pg_partman real backend tests."""

    @pytest.mark.asyncio
    async def test_pg_partman_create_parent_and_run_maintenance(
        self,
        async_postgres_backend_single,
        async_pg_partman_table,
    ):
        """pg_partman can register a parent table, update config, and maintain it.

                Scenario: production automation delegates future partition maintenance to
                pg_partman while keeping configuration changes explicit.

                Steps: register the partitioned parent, update part_config options, run
                scoped maintenance, then run global maintenance.

                Assertions: part_config stores the parent and updated options; maintenance
                calls complete; partition metadata can be inspected after maintenance.

                Production value: this verifies the pg_partman runbook used by operators
                to keep future partitions maintained without manual DDL for every window.
        """
        partman_schema = await _async_pg_partman_schema(async_postgres_backend_single)
        sql, params = _pg_partman_create_parent_sql(
            async_postgres_backend_single.dialect,
            partman_schema,
        )
        options = ExecutionOptions(stmt_type=StatementType.DQL)
        await async_postgres_backend_single.execute(sql, params, options=options)

        await async_postgres_backend_single.execute(
            *_pg_partman_update_config_sql(
                async_postgres_backend_single.dialect,
                partman_schema,
            )
        )

        config_table_sql = _pg_partman_config_table_sql(
            async_postgres_backend_single.dialect,
            partman_schema,
        )
        row = await async_postgres_backend_single.fetch_one(
            f"""
            SELECT parent_table, automatic_maintenance, infinite_time_partitions
            FROM {config_table_sql}
            WHERE parent_table = %s
            """,
            (_qualified(async_pg_partman_table),),
        )
        assert row is not None
        assert row["automatic_maintenance"] == "on"
        assert row["infinite_time_partitions"] is True

        sql, params = _pg_partman_run_maintenance_sql(
            async_postgres_backend_single.dialect,
            partman_schema,
        )
        await async_postgres_backend_single.execute(sql, params, options=options)
        sql, params = _pg_partman_global_run_maintenance_sql(
            async_postgres_backend_single.dialect,
            partman_schema,
        )
        await async_postgres_backend_single.execute(sql, params, options=options)

        metadata_sql, metadata_params = _partition_metadata_sql(
            async_postgres_backend_single.dialect,
            async_pg_partman_table,
        )
        metadata = await async_postgres_backend_single.fetch_all(metadata_sql, metadata_params)
        assert isinstance(metadata, list)
