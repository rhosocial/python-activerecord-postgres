# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_model_truncate.py
"""Live execution tests for the model-level ``truncate()`` operation."""

import pytest
import pytest_asyncio

from rhosocial.activerecord.model import ActiveRecord, AsyncActiveRecord


TABLE = "model_truncate_events"


class TestSyncModelTruncate:
    @pytest.fixture
    def table(self, postgres_backend):
        postgres_backend.execute(f"DROP TABLE IF EXISTS {TABLE}")
        postgres_backend.execute(
            f"CREATE TABLE {TABLE} (id SERIAL PRIMARY KEY, name VARCHAR(50))"
        )
        postgres_backend.execute(f"INSERT INTO {TABLE} (name) VALUES ('a'), ('b'), ('c')")
        yield
        postgres_backend.execute(f"DROP TABLE IF EXISTS {TABLE}")

    def test_truncate_removes_all_rows(self, postgres_backend, table):
        class Event(ActiveRecord):
            __table_name__ = TABLE

            id: int
            name: str

        Event.__backend__ = postgres_backend
        Event.truncate()
        assert postgres_backend.fetch_one(
            f"SELECT COUNT(*) AS cnt FROM {TABLE}"
        )["cnt"] == 0


class TestAsyncModelTruncate:
    @pytest_asyncio.fixture
    async def table(self, async_postgres_backend):
        await async_postgres_backend.execute(f"DROP TABLE IF EXISTS {TABLE}")
        await async_postgres_backend.execute(
            f"CREATE TABLE {TABLE} (id SERIAL PRIMARY KEY, name VARCHAR(50))"
        )
        await async_postgres_backend.execute(
            f"INSERT INTO {TABLE} (name) VALUES ('a'), ('b'), ('c')"
        )
        yield
        await async_postgres_backend.execute(f"DROP TABLE IF EXISTS {TABLE}")

    @pytest.mark.asyncio
    async def test_truncate_removes_all_rows(self, async_postgres_backend, table):
        class Event(AsyncActiveRecord):
            __table_name__ = TABLE

            id: int
            name: str

        Event.__backend__ = async_postgres_backend
        await Event.truncate()
        count = await async_postgres_backend.fetch_one(
            f"SELECT COUNT(*) AS cnt FROM {TABLE}"
        )
        assert count["cnt"] == 0
