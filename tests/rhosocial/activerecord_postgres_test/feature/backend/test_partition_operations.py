# tests/rhosocial/activerecord_postgres_test/feature/backend/test_partition_operations.py
"""Real PostgreSQL partition operation tests.

These tests execute against the configured PostgreSQL scenarios. They cover
common operational needs for declarative partitioning and keep synchronous and
asynchronous test method names identical across test classes.
"""
import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.expression import QualifiedIdentifierExpression
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    CreateTableExpression,
    DropTableExpression,
    PartitionKey,
    PartitionClause,
    PartitionStrategy,
    TruncateExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression import (
    PostgresAttachPartitionExpression,
    PostgresCreatePartitionExpression,
    PostgresDetachPartitionExpression,
    PostgresPgPartmanCreateParentExpression,
    PostgresPgPartmanDeleteConfigExpression,
    PostgresPgPartmanRunMaintenanceExpression,
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
            strategy=PartitionStrategy.RANGE,
            key=PartitionKey(columns=["created_at"]),
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

        metadata = postgres_backend.fetch_one(
            """
            SELECT pg_get_partkeydef(c.oid) AS partition_key
            FROM pg_class c
            WHERE c.relname = %s
            """,
            (partitioned_event_table,),
        )
        assert metadata["partition_key"] == "RANGE (created_at)"

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
            "INSERT INTO ar_partition_events (id, created_at, payload) VALUES (%s, %s, %s)",
            (3, "2026-03-15", "mar"),
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

        metadata = await async_postgres_backend.fetch_one(
            """
            SELECT pg_get_partkeydef(c.oid) AS partition_key
            FROM pg_class c
            WHERE c.relname = %s
            """,
            (async_partitioned_event_table,),
        )
        assert metadata["partition_key"] == "RANGE (created_at)"

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
            "INSERT INTO ar_partition_events (id, created_at, payload) VALUES (%s, %s, %s)",
            (3, "2026-03-15", "mar"),
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


class TestPostgreSQLPgPartmanOperations:
    """Synchronous pg_partman real backend tests."""

    def test_pg_partman_create_parent_and_run_maintenance(
        self,
        postgres_backend_single,
        pg_partman_table,
    ):
        """pg_partman can register a parent table and run scoped maintenance."""
        partman_schema = _pg_partman_schema(postgres_backend_single)
        sql, params = _pg_partman_create_parent_sql(postgres_backend_single.dialect, partman_schema)
        options = ExecutionOptions(stmt_type=StatementType.DQL)
        postgres_backend_single.execute(sql, params, options=options)

        config_table_sql = _pg_partman_config_table_sql(postgres_backend_single.dialect, partman_schema)
        row = postgres_backend_single.fetch_one(
            f"""
            SELECT parent_table
            FROM {config_table_sql}
            WHERE parent_table = %s
            """,
            (_qualified(pg_partman_table),),
        )
        assert row is not None

        sql, params = _pg_partman_run_maintenance_sql(postgres_backend_single.dialect, partman_schema)
        postgres_backend_single.execute(sql, params, options=options)


class TestAsyncPostgreSQLPgPartmanOperations:
    """Asynchronous pg_partman real backend tests."""

    @pytest.mark.asyncio
    async def test_pg_partman_create_parent_and_run_maintenance(
        self,
        async_postgres_backend_single,
        async_pg_partman_table,
    ):
        """pg_partman can register a parent table and run scoped maintenance."""
        partman_schema = await _async_pg_partman_schema(async_postgres_backend_single)
        sql, params = _pg_partman_create_parent_sql(async_postgres_backend_single.dialect, partman_schema)
        options = ExecutionOptions(stmt_type=StatementType.DQL)
        await async_postgres_backend_single.execute(sql, params, options=options)

        config_table_sql = _pg_partman_config_table_sql(async_postgres_backend_single.dialect, partman_schema)
        row = await async_postgres_backend_single.fetch_one(
            f"""
            SELECT parent_table
            FROM {config_table_sql}
            WHERE parent_table = %s
            """,
            (_qualified(async_pg_partman_table),),
        )
        assert row is not None

        sql, params = _pg_partman_run_maintenance_sql(async_postgres_backend_single.dialect, partman_schema)
        await async_postgres_backend_single.execute(sql, params, options=options)
