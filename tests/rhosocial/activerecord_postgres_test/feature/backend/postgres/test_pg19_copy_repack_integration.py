# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_pg19_copy_repack_integration.py
"""Live PostgreSQL COPY and REPACK integration tests."""

import pytest

from rhosocial.activerecord.backend.expression import Column, Literal, QueryExpression, TableExpression
from rhosocial.activerecord.backend.impl.postgres import (
    PostgresCopyFromExpression,
    PostgresCopyToExpression,
    PostgresRepackExpression,
)


TABLE_NAME = "rhosocial_copy_repack_test"
PARTITIONED_TABLE_NAME = "rhosocial_copy_partitioned_test"
PARTITION_NAME = "rhosocial_copy_partitioned_test_0_10"


def prepare_table(backend):
    backend.execute(f'DROP TABLE IF EXISTS "{TABLE_NAME}"')
    backend.execute(f'CREATE TABLE "{TABLE_NAME}" ("id" integer, "name" text)')


async def async_prepare_table(backend):
    await backend.execute(f'DROP TABLE IF EXISTS "{TABLE_NAME}"')
    await backend.execute(f'CREATE TABLE "{TABLE_NAME}" ("id" integer, "name" text)')


def test_live_copy_round_trip_and_native_repack(postgres_backend):
    dialect = postgres_backend.dialect
    prepare_table(postgres_backend)
    try:
        copied = postgres_backend.copy_from(
            PostgresCopyFromExpression(dialect, TABLE_NAME),
            [b"1\tAlice\n", b"2\tBob\n"],
        )
        assert copied == 2

        query = QueryExpression(
            dialect,
            select=[Column(dialect, "id"), Column(dialect, "name")],
            from_=TableExpression(dialect, TABLE_NAME),
            where=Column(dialect, "id") > Literal(dialect, 1),
        )
        assert postgres_backend.copy_to(PostgresCopyToExpression(dialect, query=query)) == b"2\tBob\n"

        if dialect.version >= (19, 0, 0):
            json_data = postgres_backend.copy_to(
                PostgresCopyToExpression(
                    dialect,
                    table_name=TABLE_NAME,
                    format="json",
                    force_array=True,
                )
            )
            assert b'"name":"Alice"' in json_data
            assert b'"name":"Bob"' in json_data
            postgres_backend.execute(*PostgresRepackExpression(dialect, table_name=TABLE_NAME).to_sql())
    finally:
        postgres_backend.execute(f'DROP TABLE IF EXISTS "{TABLE_NAME}"')


def test_live_pg19_partitioned_copy_to(postgres_backend):
    dialect = postgres_backend.dialect
    if dialect.version < (19, 0, 0):
        pytest.skip("Direct partitioned COPY TO requires PostgreSQL 19+")
    postgres_backend.execute(f'DROP TABLE IF EXISTS "{PARTITIONED_TABLE_NAME}"')
    postgres_backend.execute(
        f'CREATE TABLE "{PARTITIONED_TABLE_NAME}" ("id" integer, "name" text) PARTITION BY RANGE ("id")'
    )
    postgres_backend.execute(
        f'CREATE TABLE "{PARTITION_NAME}" PARTITION OF "{PARTITIONED_TABLE_NAME}" FOR VALUES FROM (0) TO (10)'
    )
    try:
        copied = postgres_backend.copy_from(
            PostgresCopyFromExpression(dialect, PARTITION_NAME),
            b"1\tPartitioned\n",
        )
        assert copied == 1
        data = postgres_backend.copy_to(
            PostgresCopyToExpression(
                dialect,
                table_name=PARTITIONED_TABLE_NAME,
                partitioned=True,
            )
        )
        assert data == b"1\tPartitioned\n"
    finally:
        postgres_backend.execute(f'DROP TABLE IF EXISTS "{PARTITIONED_TABLE_NAME}"')


@pytest.mark.asyncio
async def test_live_async_copy_round_trip_and_native_repack(async_postgres_backend):
    dialect = async_postgres_backend.dialect
    await async_prepare_table(async_postgres_backend)
    try:
        copied = await async_postgres_backend.copy_from(
            PostgresCopyFromExpression(dialect, TABLE_NAME),
            [b"1\tAlice\n", b"2\tBob\n"],
        )
        assert copied == 2

        query = QueryExpression(
            dialect,
            select=[Column(dialect, "id"), Column(dialect, "name")],
            from_=TableExpression(dialect, TABLE_NAME),
            where=Column(dialect, "id") > Literal(dialect, 1),
        )
        copied_query = await async_postgres_backend.copy_to(PostgresCopyToExpression(dialect, query=query))
        assert copied_query == b"2\tBob\n"

        if dialect.version >= (19, 0, 0):
            json_data = await async_postgres_backend.copy_to(
                PostgresCopyToExpression(
                    dialect,
                    table_name=TABLE_NAME,
                    format="json",
                    force_array=True,
                )
            )
            assert b'"name":"Alice"' in json_data
            await async_postgres_backend.execute(*PostgresRepackExpression(dialect, table_name=TABLE_NAME).to_sql())
    finally:
        await async_postgres_backend.execute(f'DROP TABLE IF EXISTS "{TABLE_NAME}"')
