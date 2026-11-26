# tests/rhosocial/activerecord_postgres_test/feature/backend/execute/postgres/extension/
#   pgroonga/test_pgroonga_integration.py
"""Integration tests for the PGroonga extension (sync and async).

Every statement is built with expression objects.  Tests skip automatically on
servers without PGroonga; on installed servers they exercise the v2 operators,
the pgroonga index access method, ranking and CJK text (the reason to prefer
PGroonga over tsvector).
"""

import pytest
import pytest_asyncio

from rhosocial.activerecord_postgres_test.feature.backend.utils import (
    ensure_extension_installed,
    async_ensure_extension_installed,
)
from rhosocial.activerecord.backend.expression import (
    CreateTableStatement,
    ColumnDefinition,
    ColumnConstraint,
    ColumnPrimaryKeyConstraint,
    InsertStatement,
    QueryStatement,
    Column,
)
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.types import TextType
from rhosocial.activerecord.backend.impl.postgres.expression.types import PostgresSerialType
from rhosocial.activerecord.backend.impl.postgres.functions.pgroonga import (
    pgroonga_highlight_html,
    pgroonga_match,
    pgroonga_match_in,
    pgroonga_query,
    pgroonga_query_escape,
    pgroonga_query_extract_keywords,
    pgroonga_score,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

DQL = ExecutionOptions(stmt_type=StatementType.DQL)
DDL = ExecutionOptions(stmt_type=StatementType.DDL)


def _create_and_fill(backend, dialect, table, suffix=""):
    """Create one search table and insert multilingual rows."""
    columns = [
        ColumnDefinition(
            dialect,
            name="id",
            data_type=PostgresSerialType(dialect=dialect),
            constraints=[ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect))],
        ),
        ColumnDefinition(dialect, name="body", data_type=TextType(dialect=dialect)),
    ]
    backend.execute(
        *CreateTableStatement(
            dialect=dialect,
            table=Table(dialect, table),
            columns=columns,
            if_not_exists=True,
        ).to_sql(),
        options=DDL,
    )
    rows = [
        [Literal(dialect, "Groonga is a fast full text search engine.")],
        [Literal(dialect, "PostgreSQL is a relational database.")],
        [Literal(dialect, "中文全文检索：PostgreSQL 与 Groonga 的组合。")],
    ]
    backend.execute(
        *InsertStatement(
            dialect=dialect,
            into=Table(dialect, table),
            columns=["body"],
            source=ValuesSource(dialect, rows),
        ).to_sql()
    )
    index = dialect.build_pgroonga_index_expression(f"idx_{table}{suffix}", table, ["body"])
    backend.execute(*index.to_sql(), options=DDL)


def _drop(backend, dialect, table, index):
    for name in (f"DROP INDEX IF EXISTS {index}", f"DROP TABLE IF EXISTS {table}"):
        backend.execute(name, (), options=DDL)


def _select_ids(backend, dialect, table, predicate, select=None):
    query = QueryStatement(
        dialect=dialect,
        select=select or [Column(dialect, "id")],
        from_=Table(dialect, table),
        where=predicate,
    )
    result = backend.execute(*query.to_sql(), options=DQL)
    return sorted(row["id"] for row in result.data)


@pytest.fixture
def pgroonga_env(postgres_backend_single):
    backend = postgres_backend_single
    ensure_extension_installed(backend, "pgroonga")
    dialect = backend.dialect
    table = "test_pgroonga_docs"
    index = "idx_test_pgroonga_docs"
    _drop(backend, dialect, table, index)
    _create_and_fill(backend, dialect, table)
    yield backend, dialect, table, index
    _drop(backend, dialect, table, index)


@pytest_asyncio.fixture
async def async_pgroonga_env(async_postgres_backend_single):
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "pgroonga")
    dialect = backend.dialect
    table = "test_pgroonga_docs_async"
    index = "idx_test_pgroonga_docs_async"
    await backend.execute(f"DROP INDEX IF EXISTS {index}", (), options=DDL)
    await backend.execute(f"DROP TABLE IF EXISTS {table}", (), options=DDL)
    columns = [
        ColumnDefinition(
            dialect,
            name="id",
            data_type=PostgresSerialType(dialect=dialect),
            constraints=[ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect))],
        ),
        ColumnDefinition(dialect, name="body", data_type=TextType(dialect=dialect)),
    ]
    await backend.execute(
        *CreateTableStatement(
            dialect=dialect, table=Table(dialect, table), columns=columns, if_not_exists=True
        ).to_sql(),
        options=DDL,
    )
    rows = [
        [Literal(dialect, "Groonga is a fast full text search engine.")],
        [Literal(dialect, "PostgreSQL is a relational database.")],
        [Literal(dialect, "中文全文检索：PostgreSQL 与 Groonga 的组合。")],
    ]
    await backend.execute(
        *InsertStatement(
            dialect=dialect,
            into=Table(dialect, table),
            columns=["body"],
            source=ValuesSource(dialect, rows),
        ).to_sql()
    )
    await backend.execute(
        *dialect.build_pgroonga_index_expression(index, table, ["body"]).to_sql(), options=DDL
    )
    yield backend, dialect, table, index
    await backend.execute(f"DROP INDEX IF EXISTS {index}", (), options=DDL)
    await backend.execute(f"DROP TABLE IF EXISTS {table}", (), options=DDL)


class TestPgRoongaIntegration:
    def test_capability_detection(self, pgroonga_env):
        backend, dialect, _, _ = pgroonga_env
        assert dialect.is_extension_installed("pgroonga")
        version = dialect.get_extension_version("pgroonga")
        assert version and tuple(int(p) for p in version.split(".")[:2]) >= (2, 0)
        assert dialect.supports_pgroonga_match()
        assert dialect.supports_pgroonga_score()

    def test_keyword_match(self, pgroonga_env):
        backend, dialect, table, _ = pgroonga_env
        assert _select_ids(backend, dialect, table, pgroonga_match(dialect, "body", "Groonga")) == [1, 3]

    def test_query_language_and_keyword_array(self, pgroonga_env):
        backend, dialect, table, _ = pgroonga_env
        query = pgroonga_query(dialect, "body", "Groonga OR PostgreSQL")
        assert _select_ids(backend, dialect, table, query) == [1, 2, 3]
        assert _select_ids(
            backend, dialect, table, pgroonga_match_in(dialect, "body", ["relational", "engine"])
        ) == [1, 2]

    def test_cjk_full_text_search(self, pgroonga_env):
        """Default TokenBigram handles Chinese without a text search config."""
        backend, dialect, table, _ = pgroonga_env
        assert _select_ids(backend, dialect, table, pgroonga_match(dialect, "body", "全文检索")) == [3]

    def test_ranking_and_highlight(self, pgroonga_env):
        backend, dialect, table, _ = pgroonga_env
        predicate = pgroonga_match(dialect, "body", "Groonga")
        query = QueryStatement(
            dialect=dialect,
            select=[Column(dialect, "id"), pgroonga_score(dialect).as_("score")],
            from_=Table(dialect, table),
            where=predicate,
        )
        # pgroonga_score() is non-zero only on an index scan; a three-row test
        # table would otherwise be scanned sequentially and score every row 0.
        backend.execute("SET enable_seqscan = off", (), options=DDL)
        try:
            result = backend.execute(*query.to_sql(), options=DQL)
            assert len(result.data) == 2
            assert all(row["score"] > 0 for row in result.data)

            highlighted = QueryStatement(
                dialect=dialect,
                select=[pgroonga_highlight_html(dialect, "body", ["Groonga"]).as_("snippet")],
                from_=Table(dialect, table),
                where=predicate,
            )
            result = backend.execute(*highlighted.to_sql(), options=DQL)
            assert any("<span" in row["snippet"] for row in result.data)
        finally:
            backend.execute("SET enable_seqscan = on", (), options=DDL)

    def test_query_helpers(self, pgroonga_env):
        backend, dialect, _, _ = pgroonga_env
        sql, params = pgroonga_query_escape(dialect, "a OR b").to_sql()
        escaped = backend.fetch_one(f"SELECT {sql} AS value", params)
        assert escaped["value"] == "a OR b"
        sql, params = pgroonga_query_extract_keywords(dialect, "groonga OR postgresql").to_sql()
        extracted = backend.fetch_one(f"SELECT {sql} AS value", params)
        assert "groonga" in extracted["value"]


class TestAsyncPgRoongaIntegration:
    @pytest.mark.asyncio
    async def test_async_extension_and_search(self, async_pgroonga_env):
        backend, dialect, table, _ = async_pgroonga_env
        assert dialect.is_extension_installed("pgroonga")
        assert await backend.fetch_one("SELECT pgroonga_score(tableoid, ctid) IS NOT NULL FROM " + table + " LIMIT 1")
        assert (await backend.fetch_one("SELECT count(*) FROM " + table))["count"] == 3

    @pytest.mark.asyncio
    async def test_async_match_and_cjk(self, async_pgroonga_env):
        backend, dialect, table, _ = async_pgroonga_env
        query = QueryStatement(
            dialect=dialect,
            select=[Column(dialect, "id")],
            from_=Table(dialect, table),
            where=pgroonga_match(dialect, "body", "全文检索"),
        )
        result = await backend.execute(*query.to_sql(), options=DQL)
        assert [row["id"] for row in result.data] == [3]

    @pytest.mark.asyncio
    async def test_async_query_language(self, async_pgroonga_env):
        backend, dialect, table, _ = async_pgroonga_env
        query = QueryStatement(
            dialect=dialect,
            select=[Column(dialect, "id")],
            from_=Table(dialect, table),
            where=pgroonga_query(dialect, "body", "Groonga OR relational"),
        )
        result = await backend.execute(*query.to_sql(), options=DQL)
        assert sorted(row["id"] for row in result.data) == [1, 2, 3]
