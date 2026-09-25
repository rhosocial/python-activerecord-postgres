# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_pg19_copy_repack.py
"""Tests for PostgreSQL COPY and native REPACK expressions and APIs."""

from contextlib import contextmanager
from unittest.mock import AsyncMock, Mock

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.errors import QueryError
from rhosocial.activerecord.backend.expression import (
    Column,
    Literal,
    QueryExpression,
    RawSQLExpression,
    TableExpression,
)
from rhosocial.activerecord.backend.expression.serialization import deserialize, serialize
from rhosocial.activerecord.backend.impl.postgres import (
    AsyncPostgresBackend,
    PostgresBackend,
    PostgresCopyFromExpression,
    PostgresCopyLogVerbosity,
    PostgresCopyOnError,
    PostgresCopyToExpression,
    PostgresDialect,
    PostgresRepackExpression,
)
from rhosocial.activerecord.backend.impl.postgres.mixins import (
    PostgresCopyMixin,
    PostgresRepackMixin,
)
from rhosocial.activerecord.backend.impl.postgres.protocols import (
    PostgresCopySupport,
    PostgresRepackSupport,
)


class SyncCopy:
    def __init__(self, chunks=(), error=None):
        self.chunks = list(chunks)
        self.error = error
        self.writes = []

    def __iter__(self):
        yield from self.chunks
        if self.error is not None:
            raise self.error

    def write(self, chunk):
        self.writes.append(chunk)


class SyncCursor:
    def __init__(self, copy, rowcount=0):
        self.copy_object = copy
        self.rowcount = rowcount
        self.statements = []
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.closed = True

    @contextmanager
    def copy(self, statement, params):
        self.statements.append((statement, params))
        yield self.copy_object


class AsyncCopy:
    def __init__(self, chunks=(), error=None):
        self.chunks = list(chunks)
        self.error = error
        self.writes = []

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk
        if self.error is not None:
            raise self.error

    async def write(self, chunk):
        self.writes.append(chunk)


class AsyncCopyContext:
    def __init__(self, copy):
        self.copy = copy

    async def __aenter__(self):
        return self.copy

    async def __aexit__(self, exc_type, exc_value, traceback):
        return None


class AsyncCursor:
    def __init__(self, copy, rowcount=0):
        self.copy_object = copy
        self.rowcount = rowcount
        self.statements = []
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.closed = True

    def copy(self, statement, params):
        self.statements.append((statement, params))
        return AsyncCopyContext(self.copy_object)


def make_sync_backend(cursor):
    backend = object.__new__(PostgresBackend)
    backend._get_cursor = Mock(return_value=cursor)
    backend._transaction_manager = None
    backend._connection = None
    return backend


def make_async_backend(cursor):
    backend = object.__new__(AsyncPostgresBackend)
    backend._get_cursor = AsyncMock(return_value=cursor)
    backend._transaction_manager = None
    backend._connection = None
    return backend


def select_query(dialect):
    return QueryExpression(
        dialect,
        select=[Column(dialect, "id")],
        from_=TableExpression(dialect, "users"),
        where=Column(dialect, "id") > Literal(dialect, 3),
    )


def test_copy_to_table_formatter_and_safe_literals():
    dialect = PostgresDialect((19, 0, 0))
    expression = PostgresCopyToExpression(
        dialect,
        table_name='users"; DROP TABLE users; --',
        schema="app data",
        columns=["id", "display'name"],
        format="csv",
        header=True,
        null="NULL",
        delimiter="|",
        quote="'",
        escape="\\",
    )

    sql, params = expression.to_sql()

    assert params == ()
    assert 'COPY "app data"."users""; DROP TABLE users; --"' in sql
    assert '("id", "display\'name")' in sql
    assert "FORMAT CSV" in sql
    assert "HEADER" in sql
    assert "NULL 'NULL'" in sql
    assert "DELIMITER '|'" in sql
    assert "QUOTE ''''" in sql
    assert "ESCAPE E'\\\\'" in sql
    assert "PROGRAM" not in sql
    assert "STDOUT" in sql


def test_copy_to_requires_table_or_base_expression_query():
    dialect = PostgresDialect((19, 0, 0))
    with pytest.raises(ValueError, match="exactly one"):
        PostgresCopyToExpression(dialect)
    with pytest.raises(TypeError, match="BaseExpression"):
        PostgresCopyToExpression(dialect, query="SELECT 1")
    with pytest.raises(ValueError, match="not both"):
        PostgresCopyToExpression(dialect, table_name="users", query=RawSQLExpression(dialect, "SELECT 1"))
    with pytest.raises(TypeError):
        PostgresCopyToExpression(dialect, table_name="users", program="cat input")


def test_copy_to_query_returns_parameters_and_rejects_mutations():
    dialect = PostgresDialect((19, 0, 0))
    expression = PostgresCopyToExpression(dialect, query=select_query(dialect))

    sql, params = expression.to_sql()

    assert sql == 'COPY (SELECT "id" FROM "users" WHERE "id" > %s) TO STDOUT (FORMAT TEXT)'
    assert params == (3,)
    with pytest.raises(ValueError, match="read-only"):
        PostgresCopyToExpression(
            dialect,
            query=RawSQLExpression(dialect, "SELECT 1; DELETE FROM users"),
        ).to_sql()
    with pytest.raises(ValueError, match="read-only"):
        PostgresCopyToExpression(
            dialect,
            query=RawSQLExpression(dialect, "WITH gone AS (DELETE FROM users RETURNING id) SELECT * FROM gone"),
        ).to_sql()


def test_copy_to_pg19_json_force_array_and_partitioned_gates():
    expression = PostgresCopyToExpression(
        PostgresDialect((18, 0, 0)),
        table_name="events",
        format="json",
        force_array=True,
        partitioned=True,
    )
    with pytest.raises(UnsupportedFeatureError, match="PostgreSQL 19"):
        expression.to_sql()

    sql, params = PostgresCopyToExpression(
        PostgresDialect((19, 0, 0)),
        table_name="events",
        format="json",
        force_array=True,
        partitioned=True,
    ).to_sql()
    assert sql == 'COPY "events" TO STDOUT (FORMAT JSON, FORCE_ARRAY)'
    assert params == ()


def test_copy_to_rejects_format_option_combinations():
    dialect = PostgresDialect((19, 0, 0))
    with pytest.raises(ValueError, match="JSON"):
        PostgresCopyToExpression(dialect, table_name="events", format="json", header=True).to_sql()
    with pytest.raises(ValueError, match="CSV"):
        PostgresCopyToExpression(dialect, table_name="events", quote='"').to_sql()
    with pytest.raises(ValueError, match="FORCE_ARRAY"):
        PostgresCopyToExpression(dialect, table_name="events", force_array=True).to_sql()
    with pytest.raises(ValueError, match="single-byte"):
        PostgresCopyToExpression(dialect, table_name="events", delimiter="ab").to_sql()


def test_copy_from_csv_formatter_with_all_supported_options():
    dialect = PostgresDialect((19, 0, 0))
    expression = PostgresCopyFromExpression(
        dialect,
        'users"; DROP TABLE users; --',
        schema="app data",
        columns=["id", "name"],
        format="csv",
        header="match",
        null="NULL",
        default="DEFAULT",
        delimiter="|",
        quote="'",
        escape="\\",
        force_not_null=["name"],
        force_null="*",
        on_error=PostgresCopyOnError.IGNORE,
        reject_limit=5,
        encoding="UTF8",
        log_verbosity=PostgresCopyLogVerbosity.SILENT,
    )

    sql, params = expression.to_sql()

    assert params == ()
    assert 'COPY "app data"."users""; DROP TABLE users; --" ("id", "name") FROM STDIN' in sql
    assert "FORMAT CSV" in sql
    assert "HEADER MATCH" in sql
    assert "NULL 'NULL'" in sql
    assert "DEFAULT 'DEFAULT'" in sql
    assert 'FORCE_NOT_NULL ("name")' in sql
    assert "FORCE_NULL *" in sql
    assert "ON_ERROR IGNORE" in sql
    assert "REJECT_LIMIT 5" in sql
    assert "ENCODING 'UTF8'" in sql
    assert "LOG_VERBOSITY SILENT" in sql
    assert "PROGRAM" not in sql


def test_copy_from_rejects_json_and_direction_options():
    dialect = PostgresDialect((19, 0, 0))
    with pytest.raises(ValueError, match="COPY FROM does not support JSON"):
        PostgresCopyFromExpression(dialect, "users", format="json").to_sql()
    with pytest.raises(TypeError):
        PostgresCopyFromExpression(dialect, "users", stdout=True)
    with pytest.raises(TypeError):
        PostgresCopyFromExpression(dialect, "users", query=RawSQLExpression(dialect, "SELECT 1"))


def test_copy_header_version_boundaries():
    with pytest.raises(UnsupportedFeatureError, match="PostgreSQL 15"):
        PostgresCopyToExpression(
            PostgresDialect((14, 0, 0)),
            table_name="users",
            header=True,
        ).to_sql()
    assert (
        "HEADER"
        in PostgresCopyToExpression(
            PostgresDialect((15, 0, 0)),
            table_name="users",
            header=True,
        ).to_sql()[0]
    )
    with pytest.raises(UnsupportedFeatureError, match="PostgreSQL 15"):
        PostgresCopyFromExpression(
            PostgresDialect((14, 0, 0)),
            "users",
            header="match",
        ).to_sql()
    assert (
        "HEADER MATCH"
        in PostgresCopyFromExpression(
            PostgresDialect((15, 0, 0)),
            "users",
            header="match",
        ).to_sql()[0]
    )
    assert (
        "HEADER FALSE"
        in PostgresCopyToExpression(
            PostgresDialect((14, 0, 0)),
            table_name="users",
            header=False,
        ).to_sql()[0]
    )
    assert (
        "HEADER"
        in PostgresCopyFromExpression(
            PostgresDialect((14, 0, 0)),
            "users",
            format="csv",
            header=True,
        ).to_sql()[0]
    )
    with pytest.raises(UnsupportedFeatureError, match="PostgreSQL 19"):
        PostgresCopyFromExpression(
            PostgresDialect((18, 0, 0)),
            "users",
            format="csv",
            header=3,
        ).to_sql()
    assert (
        "HEADER 1"
        in PostgresCopyFromExpression(
            PostgresDialect((18, 0, 0)),
            "users",
            format="csv",
            header=1,
        ).to_sql()[0]
    )
    assert (
        "HEADER 3"
        in PostgresCopyFromExpression(
            PostgresDialect((19, 0, 0)),
            "users",
            format="csv",
            header=3,
        ).to_sql()[0]
    )
    with pytest.raises(ValueError, match="0 or 1"):
        PostgresCopyToExpression(
            PostgresDialect((19, 0, 0)),
            table_name="users",
            header=2,
        ).to_sql()


def test_copy_error_option_version_boundaries():
    with pytest.raises(UnsupportedFeatureError, match="PostgreSQL 17"):
        PostgresCopyFromExpression(
            PostgresDialect((16, 0, 0)),
            "users",
            on_error="ignore",
        ).to_sql()
    with pytest.raises(UnsupportedFeatureError, match="PostgreSQL 18"):
        PostgresCopyFromExpression(
            PostgresDialect((17, 0, 0)),
            "users",
            on_error="ignore",
            reject_limit=2,
        ).to_sql()
    assert (
        "REJECT_LIMIT 2"
        in PostgresCopyFromExpression(
            PostgresDialect((18, 0, 0)),
            "users",
            on_error="ignore",
            reject_limit=2,
        ).to_sql()[0]
    )
    with pytest.raises(UnsupportedFeatureError, match="PostgreSQL 19"):
        PostgresCopyFromExpression(
            PostgresDialect((18, 0, 0)),
            "users",
            on_error="set_null",
        ).to_sql()
    with pytest.raises(UnsupportedFeatureError, match="PostgreSQL 17"):
        PostgresCopyFromExpression(
            PostgresDialect((16, 0, 0)),
            "users",
            log_verbosity="verbose",
        ).to_sql()
    assert (
        "LOG_VERBOSITY VERBOSE"
        in PostgresCopyFromExpression(
            PostgresDialect((17, 0, 0)),
            "users",
            log_verbosity="verbose",
        ).to_sql()[0]
    )
    with pytest.raises(UnsupportedFeatureError, match="PostgreSQL 18"):
        PostgresCopyFromExpression(
            PostgresDialect((17, 0, 0)),
            "users",
            log_verbosity="silent",
        ).to_sql()
    assert (
        "LOG_VERBOSITY SILENT"
        in PostgresCopyFromExpression(
            PostgresDialect((18, 0, 0)),
            "users",
            on_error="ignore",
            log_verbosity="silent",
        ).to_sql()[0]
    )


def test_copy_force_all_star_version_boundaries():
    for option in ("force_not_null", "force_null"):
        with pytest.raises(UnsupportedFeatureError, match="PostgreSQL 17"):
            PostgresCopyFromExpression(
                PostgresDialect((16, 0, 0)),
                "users",
                format="csv",
                **{option: "*"},
            ).to_sql()
        assert (
            f"{option.upper()} *"
            in PostgresCopyFromExpression(
                PostgresDialect((17, 0, 0)),
                "users",
                format="csv",
                **{option: "*"},
            ).to_sql()[0]
        )
    assert (
        "FORCE_QUOTE *"
        in PostgresCopyToExpression(
            PostgresDialect((16, 0, 0)),
            table_name="users",
            format="csv",
            force_quote="*",
        ).to_sql()[0]
    )


def test_copy_from_rejects_invalid_option_relationships():
    dialect = PostgresDialect((19, 0, 0))
    with pytest.raises(ValueError, match="REJECT_LIMIT requires"):
        PostgresCopyFromExpression(dialect, "users", reject_limit=1).to_sql()
    with pytest.raises(ValueError, match="require text or csv"):
        PostgresCopyFromExpression(
            dialect,
            "users",
            format="binary",
            on_error="ignore",
        ).to_sql()
    with pytest.raises(ValueError, match="QUOTE and ESCAPE"):
        PostgresCopyFromExpression(dialect, "users", quote='"').to_sql()
    with pytest.raises(ValueError, match="absent"):
        PostgresCopyFromExpression(
            dialect,
            "users",
            columns=["id"],
            force_null=["missing"],
            format="csv",
        ).to_sql()
    with pytest.raises(TypeError, match="BaseExpression"):
        PostgresCopyFromExpression(dialect, "users", where="id > 0")


def test_copy_from_where_returns_parameters():
    dialect = PostgresDialect((19, 0, 0))
    expression = PostgresCopyFromExpression(
        dialect,
        "users",
        where=Column(dialect, "id") > Literal(dialect, 7),
    )

    sql, params = expression.to_sql()

    assert sql.endswith('WHERE "id" > %s')
    assert params == (7,)


def test_repack_version_gate_and_all_forms():
    expression = PostgresRepackExpression(PostgresDialect((18, 0, 0)), table_name="users")
    with pytest.raises(UnsupportedFeatureError, match="PostgreSQL 19"):
        expression.to_sql()

    dialect = PostgresDialect((19, 0, 0))
    assert PostgresRepackExpression(dialect).to_sql() == ("REPACK", ())
    assert PostgresRepackExpression(
        dialect,
        verbose=True,
        all_using_index=True,
    ).to_sql() == ("REPACK (VERBOSE) USING INDEX", ())
    assert PostgresRepackExpression(
        dialect,
        table_name="users",
        concurrently=True,
        using_index=True,
    ).to_sql() == ('REPACK (CONCURRENTLY) "users" USING INDEX', ())
    assert PostgresRepackExpression(
        dialect,
        table_name="users",
        schema="app data",
        columns=["id", "name"],
        analyze=True,
        using_index='users"; DROP TABLE users; --',
    ).to_sql() == (
        'REPACK (ANALYZE) "app data"."users" ("id", "name") USING INDEX "users""; DROP TABLE users; --"',
        (),
    )


def test_repack_strict_relationships():
    dialect = PostgresDialect((19, 0, 0))
    with pytest.raises(ValueError, match="columns require ANALYZE"):
        PostgresRepackExpression(dialect, table_name="users", columns=["id"]).to_sql()
    with pytest.raises(ValueError, match="ANALYZE requires one table"):
        PostgresRepackExpression(dialect, analyze=True).to_sql()
    with pytest.raises(ValueError, match="CONCURRENTLY requires one table"):
        PostgresRepackExpression(dialect, concurrently=True).to_sql()
    with pytest.raises(ValueError, match="named REPACK index"):
        PostgresRepackExpression(dialect, using_index="users_idx").to_sql()
    with pytest.raises(ValueError, match="cannot be combined"):
        PostgresRepackExpression(
            dialect,
            table_name="users",
            all_using_index=True,
        ).to_sql()


def test_protocol_mro_and_serialization():
    dialect = PostgresDialect((19, 0, 0))
    assert isinstance(dialect, PostgresCopySupport)
    assert isinstance(dialect, PostgresRepackSupport)
    assert PostgresDialect.__mro__.index(PostgresCopyMixin) < PostgresDialect.__mro__.index(PostgresCopySupport)
    assert PostgresDialect.__mro__.index(PostgresRepackMixin) < PostgresDialect.__mro__.index(PostgresRepackSupport)

    original = PostgresCopyToExpression(dialect, query=select_query(dialect))
    restored = deserialize(serialize(original), dialect)
    assert isinstance(restored, PostgresCopyToExpression)
    assert restored.to_sql() == original.to_sql()


def test_sync_copy_to_consumes_complete_stream_and_passes_parameters():
    dialect = PostgresDialect((19, 0, 0))
    copy = SyncCopy([b"first", memoryview(b"-second"), "third"])
    cursor = SyncCursor(copy)
    backend = make_sync_backend(cursor)

    result = backend.copy_to(PostgresCopyToExpression(dialect, query=select_query(dialect)))

    assert result == b"first-secondthird"
    assert cursor.statements[0][1] == (3,)
    assert cursor.closed is True


def test_sync_copy_from_streams_chunks_and_returns_row_count():
    dialect = PostgresDialect((19, 0, 0))
    copy = SyncCopy()
    cursor = SyncCursor(copy, rowcount=3)
    backend = make_sync_backend(cursor)

    result = backend.copy_from(
        PostgresCopyFromExpression(dialect, "users", format="csv"),
        (b"1,one\n", memoryview(b"2,two\n"), "3,three\n"),
    )

    assert result == 3
    assert copy.writes == [b"1,one\n", memoryview(b"2,two\n"), "3,three\n"]
    assert cursor.closed is True


def test_sync_copy_to_client_error_rolls_back_explicit_transaction():
    dialect = PostgresDialect((19, 0, 0))
    copy = SyncCopy([b"partial"], RuntimeError("client copy failure"))
    cursor = SyncCursor(copy)
    backend = make_sync_backend(cursor)
    manager = Mock(is_active=True)
    backend._transaction_manager = manager
    backend._connection = Mock(closed=False)

    with pytest.raises(RuntimeError, match="client copy failure"):
        backend.copy_to(PostgresCopyToExpression(dialect, table_name="users"))

    manager.rollback.assert_called_once_with()


def test_sync_copy_from_rejects_str_with_explicit_encoding():
    dialect = PostgresDialect((19, 0, 0))
    copy = SyncCopy()
    cursor = SyncCursor(copy)
    backend = make_sync_backend(cursor)

    with pytest.raises(TypeError, match="ENCODING"):
        backend.copy_from(
            PostgresCopyFromExpression(dialect, "users", encoding="LATIN1"),
            "one\n",
        )

    assert copy.writes == []


def test_sync_copy_client_error_rolls_back_explicit_transaction():
    dialect = PostgresDialect((19, 0, 0))
    copy = SyncCopy()
    cursor = SyncCursor(copy)
    backend = make_sync_backend(cursor)
    manager = Mock(is_active=True)
    backend._transaction_manager = manager
    backend._connection = Mock(closed=False)

    def input_chunks():
        yield b"one\n"
        raise RuntimeError("client copy failure")

    with pytest.raises(RuntimeError, match="client copy failure"):
        backend.copy_from(PostgresCopyFromExpression(dialect, "users"), input_chunks())

    manager.rollback.assert_called_once_with()
    assert copy.writes == [b"one\n"]


@pytest.mark.asyncio
async def test_async_copy_to_client_error_rolls_back_explicit_transaction():
    dialect = PostgresDialect((19, 0, 0))
    copy = AsyncCopy([b"partial"], RuntimeError("client copy failure"))
    cursor = AsyncCursor(copy)
    backend = make_async_backend(cursor)
    manager = Mock(is_active=True)
    manager.rollback = AsyncMock()
    backend._transaction_manager = manager
    backend._connection = Mock(closed=False)
    backend._connection.rollback = AsyncMock()

    with pytest.raises(RuntimeError, match="client copy failure"):
        await backend.copy_to(PostgresCopyToExpression(dialect, table_name="users"))

    manager.rollback.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_async_copy_from_rejects_str_with_explicit_encoding():
    dialect = PostgresDialect((19, 0, 0))
    copy = AsyncCopy()
    cursor = AsyncCursor(copy)
    backend = make_async_backend(cursor)

    with pytest.raises(TypeError, match="ENCODING"):
        await backend.copy_from(
            PostgresCopyFromExpression(dialect, "users", encoding="LATIN1"),
            "one\n",
        )

    assert copy.writes == []


@pytest.mark.asyncio
async def test_async_copy_client_error_rolls_back_explicit_transaction():
    dialect = PostgresDialect((19, 0, 0))
    copy = AsyncCopy()
    cursor = AsyncCursor(copy)
    backend = make_async_backend(cursor)
    manager = Mock(is_active=True)
    manager.rollback = AsyncMock()
    backend._transaction_manager = manager
    backend._connection = Mock(closed=False)
    backend._connection.rollback = AsyncMock()

    async def input_chunks():
        yield b"one\n"
        raise RuntimeError("client copy failure")

    with pytest.raises(RuntimeError, match="client copy failure"):
        await backend.copy_from(PostgresCopyFromExpression(dialect, "users"), input_chunks())

    manager.rollback.assert_awaited_once_with()
    assert copy.writes == [b"one\n"]


def test_sync_copy_converts_psycopg_errors():
    from psycopg.errors import ProgrammingError

    class ErrorCursor(SyncCursor):
        @contextmanager
        def copy(self, statement, params):
            raise ProgrammingError("invalid COPY")
            yield

    dialect = PostgresDialect((19, 0, 0))
    cursor = ErrorCursor(SyncCopy())
    backend = make_sync_backend(cursor)
    backend._handle_error = Mock(side_effect=QueryError("converted"))

    with pytest.raises(QueryError, match="converted"):
        backend.copy_to(PostgresCopyToExpression(dialect, table_name="users"))


@pytest.mark.asyncio
async def test_async_copy_apis_are_symmetric_and_consume_streams():
    dialect = PostgresDialect((19, 0, 0))
    output_cursor = AsyncCursor(AsyncCopy([b"one", b"two"]))
    output_backend = make_async_backend(output_cursor)
    output = await output_backend.copy_to(PostgresCopyToExpression(dialect, query=select_query(dialect)))
    assert output == b"onetwo"
    assert output_cursor.statements[0][1] == (3,)
    assert output_cursor.closed is True

    input_copy = AsyncCopy()
    input_cursor = AsyncCursor(input_copy, rowcount=2)
    input_backend = make_async_backend(input_cursor)
    count = await input_backend.copy_from(
        PostgresCopyFromExpression(dialect, "users"),
        [b"one\n", b"two\n"],
    )
    assert count == 2
    assert input_copy.writes == [b"one\n", b"two\n"]
    assert input_cursor.closed is True
