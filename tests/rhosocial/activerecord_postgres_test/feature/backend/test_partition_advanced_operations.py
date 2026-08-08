"""Advanced real PostgreSQL partition capability tests."""

from __future__ import annotations

from typing import Any, Iterable

import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.expression import (
    Column,
    ColumnDefinition,
    CreateTableExpression,
    DropTableExpression,
    ExplainExpression,
    ExplainFormat,
    ExplainOptions,
    InsertExpression,
    Literal,
    LogicalPredicate,
    QueryExpression,
    RawSQLExpression,
    TableExpression,
    UpdateExpression,
    ValuesSource,
)
from rhosocial.activerecord.backend.expression.statements import PartitionClause, PartitionStrategy
from rhosocial.activerecord.backend.impl.postgres.expression import (
    PostgresAttachPartitionExpression,
    PostgresCreatePartitionExpression,
    PostgresDetachPartitionExpression,
    PostgresPartitionMetadataExpression,
)
from rhosocial.activerecord.backend.expression.types import (
    BigIntType, TextType, TimestampType,
)
from rhosocial.activerecord.backend.expression.statements import (
    ColumnConstraint, ColumnConstraintType,
)


LIST_PARENT = "ar_partition_adv_list_events"
LIST_ACTIVE = "ar_partition_adv_list_active"
LIST_CLOSED = "ar_partition_adv_list_closed"
HASH_PARENT = "ar_partition_adv_hash_events"
HASH_PARTITIONS = tuple(f"ar_partition_adv_hash_p{i}" for i in range(4))
RANGE_PARENT = "ar_partition_adv_range_events"
RANGE_JAN = "ar_partition_adv_range_jan"
RANGE_FEB = "ar_partition_adv_range_feb"
RANGE_TABLES = (RANGE_JAN, RANGE_FEB, RANGE_PARENT)
SCHEMA_A = "ar_partition_schema_a"
SCHEMA_B = "ar_partition_schema_b"
SCHEMA_PARENT = "partition_events"
SCHEMA_CHILD = "partition_events_p2026"


def _drop_table_expression(dialect, table_name: str, *, schema: str | None = None):
    table = TableExpression(dialect, table_name, schema_name=schema) if schema else table_name
    return DropTableExpression(dialect=dialect, table=table, if_exists=True, cascade=True)


def _create_list_parent_expression(dialect):
    return CreateTableExpression(
        dialect=dialect,
        table=LIST_PARENT,
        columns=[
            ColumnDefinition("id", BigIntType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("status", TextType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("payload", TextType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
        ],
        partition=PartitionClause(
            dialect=dialect,
            method=PartitionStrategy.LIST,
            keys=[Column(dialect, "status")],
        ),
    )


def _create_list_partition_sql(dialect, partition_name: str, values: Iterable[str]):
    expr = PostgresCreatePartitionExpression(
        dialect=dialect,
        partition_name=partition_name,
        parent_table=LIST_PARENT,
        partition_type="LIST",
        partition_values={"values": list(values)},
    )
    return expr.to_sql()


def _create_hash_parent_expression(dialect):
    return CreateTableExpression(
        dialect=dialect,
        table=HASH_PARENT,
        columns=[
            ColumnDefinition("id", BigIntType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("bucket", BigIntType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("payload", TextType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
        ],
        partition=PartitionClause(
            dialect=dialect,
            method=PartitionStrategy.HASH,
            keys=[Column(dialect, "bucket")],
        ),
    )


def _create_hash_partition_sql(dialect, partition_name: str, remainder: int):
    expr = PostgresCreatePartitionExpression(
        dialect=dialect,
        partition_name=partition_name,
        parent_table=HASH_PARENT,
        partition_type="HASH",
        partition_values={"modulus": 4, "remainder": remainder},
    )
    return expr.to_sql()


def _create_range_parent_expression(dialect):
    return CreateTableExpression(
        dialect=dialect,
        table=RANGE_PARENT,
        columns=[
            ColumnDefinition("id", BigIntType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("created_at", TimestampType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("payload", TextType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
        ],
        partition=PartitionClause(
            dialect=dialect,
            method=PartitionStrategy.RANGE,
            keys=[Column(dialect, "created_at")],
        ),
    )


def _create_range_partition_sql(dialect, partition_name: str, start: str, end: str):
    expr = PostgresCreatePartitionExpression(
        dialect=dialect,
        partition_name=partition_name,
        parent_table=RANGE_PARENT,
        partition_type="RANGE",
        partition_values={"from": start, "to": end},
    )
    return expr.to_sql()


def _insert_rows_expression(dialect, table_name: str, columns: list[str], rows: list[list[Any]]):
    return InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=columns,
        source=ValuesSource(
            dialect,
            [[Literal(dialect, value) for value in row] for row in rows],
        ),
    )


def _select_tableoid_payload_expression(dialect, table_name: str):
    return QueryExpression(
        dialect,
        select=[
            RawSQLExpression(dialect, "tableoid::regclass::text AS partition_name"),
            Column(dialect, "payload"),
        ],
        from_=TableExpression(dialect, table_name),
    )


def _metadata_sql(dialect, parent_table: str, *, schema: str | None = None, include_partitions: bool = True):
    expr = PostgresPartitionMetadataExpression(
        dialect=dialect,
        parent_table=parent_table,
        schema=schema,
        include_partitions=include_partitions,
    )
    return expr.to_sql()


def _json_explain_sql(dialect, query):
    expr = ExplainExpression(
        dialect,
        statement=query,
        options=ExplainOptions(format=ExplainFormat.JSON),
    )
    return expr.to_sql()


def _extract_json_plan(rows):
    assert rows
    row = rows[0]
    raw = row.get("QUERY PLAN") or row.get("query plan") or next(iter(row.values()))
    if isinstance(raw, str):
        import json

        raw = json.loads(raw)
    assert isinstance(raw, list)
    return raw[0]["Plan"]


def _flatten_plan_nodes(node):
    nodes = [node]
    for child in node.get("Plans", []) or []:
        nodes.extend(_flatten_plan_nodes(child))
    return nodes


def _drop_advanced_tables(backend):
    for table_name in (LIST_ACTIVE, LIST_CLOSED, LIST_PARENT, *HASH_PARTITIONS, HASH_PARENT, *RANGE_TABLES):
        backend.execute(*_drop_table_expression(backend.dialect, table_name).to_sql())


async def _async_drop_advanced_tables(backend):
    for table_name in (LIST_ACTIVE, LIST_CLOSED, LIST_PARENT, *HASH_PARTITIONS, HASH_PARENT, *RANGE_TABLES):
        await backend.execute(*_drop_table_expression(backend.dialect, table_name).to_sql())


def _create_range_environment(backend):
    dialect = backend.dialect
    for table_name in RANGE_TABLES:
        backend.execute(*_drop_table_expression(dialect, table_name).to_sql())
    backend.execute(*_create_range_parent_expression(dialect).to_sql())
    backend.execute(*_create_range_partition_sql(dialect, RANGE_JAN, "2026-01-01", "2026-02-01"))
    backend.execute(*_create_range_partition_sql(dialect, RANGE_FEB, "2026-02-01", "2026-03-01"))


async def _async_create_range_environment(backend):
    dialect = backend.dialect
    for table_name in RANGE_TABLES:
        await backend.execute(*_drop_table_expression(dialect, table_name).to_sql())
    await backend.execute(*_create_range_parent_expression(dialect).to_sql())
    await backend.execute(*_create_range_partition_sql(dialect, RANGE_JAN, "2026-01-01", "2026-02-01"))
    await backend.execute(*_create_range_partition_sql(dialect, RANGE_FEB, "2026-02-01", "2026-03-01"))


@pytest.fixture
def postgres_range_partition_environment(postgres_backend):
    """Create a small range partition environment for advanced behavior tests."""
    if not postgres_backend.dialect.supports_partitioned_table_creation():
        pytest.skip("PostgreSQL scenario does not support declarative partitioning")
    _create_range_environment(postgres_backend)
    yield RANGE_PARENT
    _drop_advanced_tables(postgres_backend)


@pytest_asyncio.fixture
async def async_postgres_range_partition_environment(async_postgres_backend):
    """Async range partition environment for advanced behavior tests."""
    if not async_postgres_backend.dialect.supports_partitioned_table_creation():
        pytest.skip("PostgreSQL scenario does not support declarative partitioning")
    await _async_create_range_environment(async_postgres_backend)
    yield RANGE_PARENT
    await _async_drop_advanced_tables(async_postgres_backend)


class TestPostgreSQLAdvancedPartitionOperations:
    """Synchronous advanced PostgreSQL partition behavior tests."""

    def test_list_partition_routes_rows_and_reports_metadata(self, postgres_backend):
        """LIST partitions should route rows and appear in metadata."""
        dialect = postgres_backend.dialect
        if not dialect.supports_list_table_partitioning():
            pytest.skip("PostgreSQL scenario does not support LIST partitioning")
        _drop_advanced_tables(postgres_backend)
        try:
            postgres_backend.execute(*_create_list_parent_expression(dialect).to_sql())
            postgres_backend.execute(*_create_list_partition_sql(dialect, LIST_ACTIVE, ["active", "pending"]))
            postgres_backend.execute(*_create_list_partition_sql(dialect, LIST_CLOSED, ["closed", "archived"]))
            postgres_backend.execute(
                *_insert_rows_expression(
                    dialect,
                    LIST_PARENT,
                    ["id", "status", "payload"],
                    [[1, "active", "active-row"], [2, "closed", "closed-row"]],
                ).to_sql()
            )
            rows = postgres_backend.fetch_all(*_select_tableoid_payload_expression(dialect, LIST_PARENT).to_sql())
            by_payload = {row["payload"]: row["partition_name"] for row in rows}
            assert by_payload == {"active-row": LIST_ACTIVE, "closed-row": LIST_CLOSED}
            metadata = postgres_backend.fetch_all(*_metadata_sql(dialect, LIST_PARENT))
            assert {row["name"] for row in metadata} == {LIST_ACTIVE, LIST_CLOSED}
        finally:
            _drop_advanced_tables(postgres_backend)

    def test_hash_partition_accepts_rows_and_reports_metadata(self, postgres_backend):
        """HASH partitions should accept rows and expose all remainder partitions."""
        dialect = postgres_backend.dialect
        if not dialect.supports_hash_table_partitioning():
            pytest.skip("PostgreSQL scenario does not support HASH partitioning")
        _drop_advanced_tables(postgres_backend)
        try:
            postgres_backend.execute(*_create_hash_parent_expression(dialect).to_sql())
            for remainder, partition_name in enumerate(HASH_PARTITIONS):
                postgres_backend.execute(*_create_hash_partition_sql(dialect, partition_name, remainder))
            postgres_backend.execute(
                *_insert_rows_expression(
                    dialect,
                    HASH_PARENT,
                    ["id", "bucket", "payload"],
                    [[1, 10, "one"], [2, 20, "two"], [3, 30, "three"]],
                ).to_sql()
            )
            metadata = postgres_backend.fetch_all(*_metadata_sql(dialect, HASH_PARENT))
            count = postgres_backend.fetch_one(
                "SELECT COUNT(*) AS count FROM ar_partition_adv_hash_events"
            )["count"]
            assert count == 3
            assert {row["name"] for row in metadata} == set(HASH_PARTITIONS)
            assert all("modulus 4" in str(row["bound"]).lower() for row in metadata)
        finally:
            _drop_advanced_tables(postgres_backend)

    def test_default_attach_and_detach_workflow(self, postgres_backend, postgres_range_partition_environment):
        """DEFAULT ATTACH should create a catch-all partition, then DETACH should remove it."""
        dialect = postgres_backend.dialect
        if not dialect.supports_default_partition():
            pytest.skip("DEFAULT partition requires PostgreSQL 11+")

        default_partition_name = "ar_partition_adv_default"

        # Create a table with matching structure for DEFAULT attachment
        create_default = CreateTableExpression(
            dialect=dialect,
            table=default_partition_name,
            columns=[
                ColumnDefinition("id", BigIntType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
                ColumnDefinition("created_at", TimestampType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
                ColumnDefinition("payload", TextType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ],
        )
        postgres_backend.execute(*create_default.to_sql())

        try:
            # Attach as DEFAULT partition
            attach = PostgresAttachPartitionExpression(
                dialect=dialect,
                partition_name=default_partition_name,
                parent_table=RANGE_PARENT,
                partition_type="RANGE",
                partition_values={"default": True},
            )
            sql, params = attach.to_sql()
            assert "DEFAULT" in sql
            assert "FOR VALUES" not in sql
            postgres_backend.execute(sql, params)

            # Verify in metadata
            metadata = postgres_backend.fetch_all(
                *_metadata_sql(dialect, RANGE_PARENT)
            )
            names = {row["name"] for row in metadata}
            assert default_partition_name in names

            # Detach it back
            detach = PostgresDetachPartitionExpression(
                dialect=dialect,
                partition_name=default_partition_name,
                parent_table=RANGE_PARENT,
            )
            postgres_backend.execute(*detach.to_sql())

            metadata_after = postgres_backend.fetch_all(
                *_metadata_sql(dialect, RANGE_PARENT)
            )
            names_after = {row["name"] for row in metadata_after}
            assert default_partition_name not in names_after
        finally:
            postgres_backend.execute(
                f'DROP TABLE IF EXISTS "{default_partition_name}" CASCADE'
            )

    def test_metadata_include_partitions_false_and_schema_filter(self, postgres_backend):
        """Metadata should distinguish schemas and parent-only queries."""
        dialect = postgres_backend.dialect
        if not dialect.supports_partition_metadata_introspection():
            pytest.skip("PostgreSQL scenario does not support partition metadata")
        for schema in (SCHEMA_A, SCHEMA_B):
            postgres_backend.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            postgres_backend.execute(f'CREATE SCHEMA "{schema}"')
        try:
            for schema in (SCHEMA_A, SCHEMA_B):
                parent = CreateTableExpression(
                    dialect=dialect,
                    table=TableExpression(dialect, SCHEMA_PARENT, schema_name=schema),
                    columns=[
                        ColumnDefinition("id", BigIntType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
                        ColumnDefinition("created_at", TimestampType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
                    ],
                    partition=PartitionClause(
                        dialect=dialect,
                        method=PartitionStrategy.RANGE,
                        keys=[Column(dialect, "created_at")],
                    ),
                )
                postgres_backend.execute(*parent.to_sql())
                child = PostgresCreatePartitionExpression(
                    dialect=dialect,
                    partition_name=SCHEMA_CHILD,
                    parent_table=SCHEMA_PARENT,
                    partition_type="RANGE",
                    partition_values={"from": "2026-01-01", "to": "2027-01-01"},
                    schema=schema,
                )
                postgres_backend.execute(*child.to_sql())
            rows_a = postgres_backend.fetch_all(
                *_metadata_sql(dialect, SCHEMA_PARENT, schema=SCHEMA_A)
            )
            rows_b_parent_only = postgres_backend.fetch_all(
                *_metadata_sql(dialect, SCHEMA_PARENT, schema=SCHEMA_B, include_partitions=False)
            )
            assert {row["name"] for row in rows_a} == {SCHEMA_CHILD}
            assert len(rows_b_parent_only) == 1
            assert rows_b_parent_only[0]["name"] is None
            assert rows_b_parent_only[0]["bound"] is None
        finally:
            for schema in (SCHEMA_A, SCHEMA_B):
                postgres_backend.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')

    def test_partition_key_update_moves_row_between_partitions(
        self,
        postgres_backend,
        postgres_range_partition_environment,
    ):
        """Updating the partition key should move rows between partitions on PG 11+."""
        dialect = postgres_backend.dialect
        if not dialect.supports_partition_key_update():
            pytest.skip("PostgreSQL scenario does not support partition key row movement")
        postgres_backend.execute(
            *_insert_rows_expression(
                dialect,
                RANGE_PARENT,
                ["id", "created_at", "payload"],
                [[1, "2026-01-15", "jan"]],
            ).to_sql()
        )
        update = UpdateExpression(
            dialect=dialect,
            table=RANGE_PARENT,
            assignments={"created_at": Literal(dialect, "2026-02-15")},
            where=Column(dialect, "id") == Literal(dialect, 1),
        )
        postgres_backend.execute(*update.to_sql())
        row = postgres_backend.fetch_one(
            "SELECT tableoid::regclass::text AS partition_name FROM ar_partition_adv_range_events WHERE id = %s",
            (1,),
        )
        assert row["partition_name"] == RANGE_FEB

        invalid_update = UpdateExpression(
            dialect=dialect,
            table=RANGE_PARENT,
            assignments={"created_at": Literal(dialect, "2026-03-15")},
            where=Column(dialect, "id") == Literal(dialect, 1),
        )
        with pytest.raises(Exception):
            postgres_backend.execute(*invalid_update.to_sql())

    def test_json_explain_pruning_reports_target_partition(
        self,
        postgres_backend,
        postgres_range_partition_environment,
    ):
        """EXPLAIN FORMAT JSON should expose pruning to the target partition."""
        dialect = postgres_backend.dialect
        postgres_backend.execute(
            *_insert_rows_expression(
                dialect,
                RANGE_PARENT,
                ["id", "created_at", "payload"],
                [[1, "2026-01-15", "jan"], [2, "2026-02-15", "feb"]],
            ).to_sql()
        )
        query = QueryExpression(
            dialect,
            select=[Column(dialect, "payload")],
            from_=TableExpression(dialect, RANGE_PARENT),
            where=LogicalPredicate(
                dialect,
                "AND",
                Column(dialect, "created_at") >= Literal(dialect, "2026-02-01"),
                Column(dialect, "created_at") < Literal(dialect, "2026-03-01"),
            ),
        )
        rows = postgres_backend.fetch_all(*_json_explain_sql(dialect, query))
        nodes = _flatten_plan_nodes(_extract_json_plan(rows))
        relation_names = {node.get("Relation Name") for node in nodes if node.get("Relation Name")}
        assert RANGE_FEB in relation_names
        assert RANGE_JAN not in relation_names

    def test_concurrent_detach_executes_or_reports_environment_limit(
        self,
        postgres_backend,
        postgres_range_partition_environment,
    ):
        """DETACH CONCURRENTLY should execute on PG 14+ outside restricted transactions."""
        dialect = postgres_backend.dialect
        if not dialect.supports_concurrent_detach():
            pytest.skip("DETACH CONCURRENTLY requires PostgreSQL 14+")
        sql, params = PostgresDetachPartitionExpression(
            dialect=dialect,
            partition_name=RANGE_JAN,
            parent_table=RANGE_PARENT,
            concurrently=True,
        ).to_sql()
        try:
            postgres_backend.execute(sql, params)
        except Exception as exc:
            if "transaction block" in str(exc).lower():
                pytest.skip("backend connection wraps DDL in a transaction block")
            raise
        metadata = postgres_backend.fetch_all(*_metadata_sql(dialect, RANGE_PARENT))
        assert RANGE_JAN not in {row["name"] for row in metadata}


class TestAsyncPostgreSQLAdvancedPartitionOperations:
    """Asynchronous advanced PostgreSQL partition behavior tests."""

    @pytest.mark.asyncio
    async def test_list_partition_routes_rows_and_reports_metadata(self, async_postgres_backend):
        """LIST partitions should route rows asynchronously."""
        dialect = async_postgres_backend.dialect
        if not dialect.supports_list_table_partitioning():
            pytest.skip("PostgreSQL scenario does not support LIST partitioning")
        await _async_drop_advanced_tables(async_postgres_backend)
        try:
            await async_postgres_backend.execute(*_create_list_parent_expression(dialect).to_sql())
            await async_postgres_backend.execute(*_create_list_partition_sql(dialect, LIST_ACTIVE, ["active", "pending"]))
            await async_postgres_backend.execute(*_create_list_partition_sql(dialect, LIST_CLOSED, ["closed", "archived"]))
            await async_postgres_backend.execute(
                *_insert_rows_expression(
                    dialect,
                    LIST_PARENT,
                    ["id", "status", "payload"],
                    [[1, "active", "active-row"], [2, "closed", "closed-row"]],
                ).to_sql()
            )
            rows = await async_postgres_backend.fetch_all(*_select_tableoid_payload_expression(dialect, LIST_PARENT).to_sql())
            by_payload = {row["payload"]: row["partition_name"] for row in rows}
            assert by_payload == {"active-row": LIST_ACTIVE, "closed-row": LIST_CLOSED}
        finally:
            await _async_drop_advanced_tables(async_postgres_backend)

    @pytest.mark.asyncio
    async def test_partition_key_update_moves_row_between_partitions(
        self,
        async_postgres_backend,
        async_postgres_range_partition_environment,
    ):
        """Updating the partition key should move rows asynchronously on PG 11+."""
        dialect = async_postgres_backend.dialect
        if not dialect.supports_partition_key_update():
            pytest.skip("PostgreSQL scenario does not support partition key row movement")
        await async_postgres_backend.execute(
            *_insert_rows_expression(
                dialect,
                RANGE_PARENT,
                ["id", "created_at", "payload"],
                [[1, "2026-01-15", "jan"]],
            ).to_sql()
        )
        update = UpdateExpression(
            dialect=dialect,
            table=RANGE_PARENT,
            assignments={"created_at": Literal(dialect, "2026-02-15")},
            where=Column(dialect, "id") == Literal(dialect, 1),
        )
        await async_postgres_backend.execute(*update.to_sql())
        row = await async_postgres_backend.fetch_one(
            "SELECT tableoid::regclass::text AS partition_name FROM ar_partition_adv_range_events WHERE id = %s",
            (1,),
        )
        assert row["partition_name"] == RANGE_FEB
