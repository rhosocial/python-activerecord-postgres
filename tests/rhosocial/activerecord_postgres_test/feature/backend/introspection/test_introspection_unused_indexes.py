# tests/rhosocial/activerecord_postgres_test/feature/backend/introspection/test_introspection_unused_indexes.py
"""Unused-indexes listing: statement shape and live execution.

The family reads two catalog relations that answer two different questions:
``pg_stat_user_indexes`` carries the counters (``idx_scan``), while the
validity flag ``indisvalid`` lives on ``pg_index``. Pointing the flag at the
statistics view makes the server reject the whole statement at parse time
(``column i.indisvalid does not exist``), so the shape assertions below pin
the relation each column belongs to, and the live cases prove the statement
parses on a real server.
"""

import pytest

from rhosocial.activerecord.backend.expression.introspection import (
    UnusedIndexesQueryStatement,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.introspection.types import UnusedIndexInfo


def _sql(version=(17, 0, 0), schema="public"):
    return UnusedIndexesQueryStatement(
        PostgresDialect(version=version), schema=schema
    ).to_sql()


class TestUnusedIndexesStatementShape:
    """The rendered statement must name columns on the relations that own them."""

    def test_validity_comes_from_pg_index(self):
        sql, _ = _sql()
        assert "pg_index AS ix" in sql
        assert "ix.indisvalid = TRUE" in sql
        assert "i.indisvalid" not in sql

    def test_index_join_uses_the_index_oid_on_both_sides(self):
        sql, _ = _sql()
        assert "i.indexrelid = ix.indexrelid" in sql
        # ``relid`` is the table oid; joining on it would pair the wrong rows.
        assert "i.relid = ix" not in sql

    def test_counter_filter_stays_on_the_statistics_view(self):
        sql, _ = _sql()
        assert "i.idx_scan = %s" in sql
        assert "i.idx_scan" in sql

    def test_schema_filter_and_projections_are_unchanged(self):
        sql, params = _sql(schema="public")
        assert "n.nspname = %s" in sql
        assert "n.nspname AS \"schema_name\"" in sql
        assert "i.relname AS \"table_name\"" in sql
        assert "i.indexrelname AS \"index_name\"" in sql
        assert params == ("public", 0)


class TestUnusedIndexesLive:
    """The statement has to parse on a real PostgreSQL server."""

    def test_list_unused_indexes_answers_for_a_real_schema(self, backend_with_tables):
        indexes = backend_with_tables.introspector.list_unused_indexes(schema=None)
        assert isinstance(indexes, list)
        for index in indexes:
            assert isinstance(index, UnusedIndexInfo)
            assert index.schema
            assert index.table_name
            assert index.index_name


class TestAsyncUnusedIndexesLive:
    """Asynchronous counterpart of the live case (same method name)."""

    @pytest.mark.asyncio
    async def test_list_unused_indexes_answers_for_a_real_schema(
        self, async_backend_with_tables
    ):
        indexes = await async_backend_with_tables.introspector.list_unused_indexes(
            schema=None
        )
        assert isinstance(indexes, list)
        for index in indexes:
            assert isinstance(index, UnusedIndexInfo)
            assert index.schema
            assert index.table_name
            assert index.index_name
