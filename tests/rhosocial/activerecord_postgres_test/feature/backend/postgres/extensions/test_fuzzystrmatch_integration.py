# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_fuzzystrmatch_integration.py
"""
Integration tests for PostgreSQL fuzzystrmatch extension with real database.

These tests require a live PostgreSQL connection with fuzzystrmatch extension installed
and test:
- levenshtein() distance function
- soundex() encoding
- difference() function
- metaphone() function
- dmetaphone() function

All database operations use expression objects, not raw SQL strings.
"""

import pytest
import pytest_asyncio

from rhosocial.activerecord_postgres_test.feature.backend.utils import (
    ensure_extension_installed,
    async_ensure_extension_installed,
)
from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
    DropTableExpression,
    InsertExpression,
    QueryExpression,
    TableExpression,
    Column,
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal, FunctionCall
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


TABLE_NAME = "test_fuzzy_lev"
TABLE_NAME_ASYNC = "test_fuzzy_lev_async"
TABLE_NAME_DIFF = "test_fuzzy_diff"
TABLE_NAME_DIFF_ASYNC = "test_fuzzy_diff_async"


def _setup_lev_table(backend, dialect, table_name):
    """Create and populate the levenshtein test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="word", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    rows = [
        [Literal(dialect, "kitten")],
        [Literal(dialect, "sitting")],
        [Literal(dialect, "sunday")],
        [Literal(dialect, "saturday")],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["word"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _setup_diff_table(backend, dialect, table_name):
    """Create and populate the difference test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    rows = [
        [Literal(dialect, "Smith")],
        [Literal(dialect, "Smythe")],
        [Literal(dialect, "Johnson")],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["name"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _teardown_table(backend, dialect, table_name):
    """Drop a test table using expression."""
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name=table_name,
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    backend.execute(sql, params)


async def _async_setup_lev_table(backend, dialect, table_name):
    """Async: create and populate the levenshtein test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="word", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    rows = [
        [Literal(dialect, "kitten")],
        [Literal(dialect, "sitting")],
        [Literal(dialect, "sunday")],
        [Literal(dialect, "saturday")],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["word"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_setup_diff_table(backend, dialect, table_name):
    """Async: create and populate the difference test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    rows = [
        [Literal(dialect, "Smith")],
        [Literal(dialect, "Smythe")],
        [Literal(dialect, "Johnson")],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["name"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_teardown_table(backend, dialect, table_name):
    """Async: drop a test table using expression."""
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name=table_name,
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    await backend.execute(sql, params)


@pytest.fixture
def fuzzystrmatch_env(postgres_backend_single):
    """Independent test environment for fuzzystrmatch extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "fuzzystrmatch")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in [TABLE_NAME, TABLE_NAME_DIFF]:
        _teardown_table(backend, dialect, table_name)

    _setup_lev_table(backend, dialect, TABLE_NAME)
    _setup_diff_table(backend, dialect, TABLE_NAME_DIFF)

    yield backend, dialect

    _teardown_table(backend, dialect, TABLE_NAME)
    _teardown_table(backend, dialect, TABLE_NAME_DIFF)


class TestFuzzystrmatchIntegration:
    """Integration tests for fuzzystrmatch extension."""

    def test_levenshtein(self, fuzzystrmatch_env):
        """Test levenshtein() distance function."""
        backend, dialect = fuzzystrmatch_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Levenshtein distance between 'kitten' and 'sitting' is 3
        func = FunctionCall(
            dialect, "levenshtein",
            Literal(dialect, "kitten"),
            Literal(dialect, "sitting"),
        ).as_("dist")
        query = QueryExpression(dialect=dialect, select=[func])
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data[0]["dist"] == 3

        # Levenshtein distance between 'sunday' and 'saturday' is 3
        func = FunctionCall(
            dialect, "levenshtein",
            Literal(dialect, "sunday"),
            Literal(dialect, "saturday"),
        ).as_("dist")
        query = QueryExpression(dialect=dialect, select=[func])
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data[0]["dist"] == 3

        # Same word should have distance 0
        func = FunctionCall(
            dialect, "levenshtein",
            Literal(dialect, "hello"),
            Literal(dialect, "hello"),
        ).as_("dist")
        query = QueryExpression(dialect=dialect, select=[func])
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data[0]["dist"] == 0

        # Find words within Levenshtein distance of 3 from 'kitten'
        lev_func = FunctionCall(
            dialect, "levenshtein",
            Literal(dialect, "kitten"),
            Column(dialect, "word"),
        )
        where_pred = ComparisonPredicate(
            dialect, "<=",
            lev_func,
            Literal(dialect, 3),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "word")],
            from_=TableExpression(dialect, TABLE_NAME),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        words = [row["word"] for row in result.data]
        assert "kitten" in words
        assert "sitting" in words

    def test_soundex(self, fuzzystrmatch_env):
        """Test soundex() encoding function."""
        backend, dialect = fuzzystrmatch_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Homophones should have the same soundex code
        func1 = FunctionCall(dialect, "soundex", Literal(dialect, "Robert")).as_("code")
        query = QueryExpression(dialect=dialect, select=[func1])
        sql, params = query.to_sql()
        result1 = backend.execute(sql, params, options=opts)

        func2 = FunctionCall(dialect, "soundex", Literal(dialect, "Rupert")).as_("code")
        query = QueryExpression(dialect=dialect, select=[func2])
        sql, params = query.to_sql()
        result2 = backend.execute(sql, params, options=opts)
        assert result1.data[0]["code"] == result2.data[0]["code"]

        # Note: Ashcraft and Ashcroft have the same soundex
        func3 = FunctionCall(dialect, "soundex", Literal(dialect, "Ashcraft")).as_("code")
        query = QueryExpression(dialect=dialect, select=[func3])
        sql, params = query.to_sql()
        result3 = backend.execute(sql, params, options=opts)

        func4 = FunctionCall(dialect, "soundex", Literal(dialect, "Ashcroft")).as_("code")
        query = QueryExpression(dialect=dialect, select=[func4])
        sql, params = query.to_sql()
        result4 = backend.execute(sql, params, options=opts)
        assert result3.data[0]["code"] == result4.data[0]["code"]

        # soundex code is 4 characters: letter + 3 digits
        func = FunctionCall(dialect, "soundex", Literal(dialect, "Hello")).as_("code")
        query = QueryExpression(dialect=dialect, select=[func])
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        code = result.data[0]["code"]
        assert len(code) == 4
        assert code[0].isalpha()
        assert code[1:].isdigit()

    def test_difference(self, fuzzystrmatch_env):
        """Test difference() function (0-4 similarity based on soundex)."""
        backend, dialect = fuzzystrmatch_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Smith and Smythe should have high similarity
        func = FunctionCall(
            dialect, "difference",
            Literal(dialect, "Smith"),
            Literal(dialect, "Smythe"),
        ).as_("diff")
        query = QueryExpression(dialect=dialect, select=[func])
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data[0]["diff"] >= 3  # High similarity

        # Smith and Johnson should have low similarity
        func = FunctionCall(
            dialect, "difference",
            Literal(dialect, "Smith"),
            Literal(dialect, "Johnson"),
        ).as_("diff")
        query = QueryExpression(dialect=dialect, select=[func])
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data[0]["diff"] <= 2  # Low similarity

        # Find names that sound similar to 'Smith'
        diff_func = FunctionCall(
            dialect, "difference",
            Column(dialect, "name"),
            Literal(dialect, "Smith"),
        )
        where_pred = ComparisonPredicate(
            dialect, ">=",
            diff_func,
            Literal(dialect, 3),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "name")],
            from_=TableExpression(dialect, TABLE_NAME_DIFF),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        names = [row["name"] for row in result.data]
        assert "Smith" in names
        assert "Smythe" in names

    def test_metaphone(self, fuzzystrmatch_env):
        """Test metaphone() function."""
        backend, dialect = fuzzystrmatch_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        func = FunctionCall(
            dialect, "metaphone",
            Literal(dialect, "hello"),
            Literal(dialect, 6),
        ).as_("meta")
        query = QueryExpression(dialect=dialect, select=[func])
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data[0]["meta"] is not None
        assert len(result.data[0]["meta"]) <= 6

        # Words with similar pronunciation should have similar metaphone codes
        func1 = FunctionCall(
            dialect, "metaphone",
            Literal(dialect, "phone"),
            Literal(dialect, 6),
        ).as_("meta")
        query = QueryExpression(dialect=dialect, select=[func1])
        sql, params = query.to_sql()
        result1 = backend.execute(sql, params, options=opts)

        func2 = FunctionCall(
            dialect, "metaphone",
            Literal(dialect, "fone"),
            Literal(dialect, 6),
        ).as_("meta")
        query = QueryExpression(dialect=dialect, select=[func2])
        sql, params = query.to_sql()
        result2 = backend.execute(sql, params, options=opts)
        assert result1.data[0]["meta"] == result2.data[0]["meta"]

    def test_dmetaphone(self, fuzzystrmatch_env):
        """Test dmetaphone() function (double metaphone)."""
        backend, dialect = fuzzystrmatch_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        func = FunctionCall(
            dialect, "dmetaphone",
            Literal(dialect, "hello"),
        ).as_("dmeta")
        query = QueryExpression(dialect=dialect, select=[func])
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data[0]["dmeta"] is not None

        # dmetaphone provides better encoding for various languages
        func1 = FunctionCall(
            dialect, "dmetaphone",
            Literal(dialect, "Phillip"),
        ).as_("dmeta")
        query = QueryExpression(dialect=dialect, select=[func1])
        sql, params = query.to_sql()
        result1 = backend.execute(sql, params, options=opts)

        func2 = FunctionCall(
            dialect, "dmetaphone",
            Literal(dialect, "Filip"),
        ).as_("dmeta")
        query = QueryExpression(dialect=dialect, select=[func2])
        sql, params = query.to_sql()
        result2 = backend.execute(sql, params, options=opts)
        # Both should produce similar (or same) dmetaphone codes
        assert result1.data[0]["dmeta"] is not None
        assert result2.data[0]["dmeta"] is not None


@pytest_asyncio.fixture
async def async_fuzzystrmatch_env(async_postgres_backend_single):
    """Independent async test environment for fuzzystrmatch extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "fuzzystrmatch")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in [TABLE_NAME_ASYNC, TABLE_NAME_DIFF_ASYNC]:
        await _async_teardown_table(backend, dialect, table_name)

    await _async_setup_lev_table(backend, dialect, TABLE_NAME_ASYNC)
    await _async_setup_diff_table(backend, dialect, TABLE_NAME_DIFF_ASYNC)

    yield backend, dialect

    await _async_teardown_table(backend, dialect, TABLE_NAME_ASYNC)
    await _async_teardown_table(backend, dialect, TABLE_NAME_DIFF_ASYNC)


class TestAsyncFuzzystrmatchIntegration:
    """Async integration tests for fuzzystrmatch extension."""

    @pytest.mark.asyncio
    async def test_async_levenshtein(self, async_fuzzystrmatch_env):
        """Test levenshtein() distance function."""
        backend, dialect = async_fuzzystrmatch_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Levenshtein distance between 'kitten' and 'sitting' is 3
        func = FunctionCall(
            dialect, "levenshtein",
            Literal(dialect, "kitten"),
            Literal(dialect, "sitting"),
        ).as_("dist")
        query = QueryExpression(dialect=dialect, select=[func])
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data[0]["dist"] == 3

        # Levenshtein distance between 'sunday' and 'saturday' is 3
        func = FunctionCall(
            dialect, "levenshtein",
            Literal(dialect, "sunday"),
            Literal(dialect, "saturday"),
        ).as_("dist")
        query = QueryExpression(dialect=dialect, select=[func])
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data[0]["dist"] == 3

        # Same word should have distance 0
        func = FunctionCall(
            dialect, "levenshtein",
            Literal(dialect, "hello"),
            Literal(dialect, "hello"),
        ).as_("dist")
        query = QueryExpression(dialect=dialect, select=[func])
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data[0]["dist"] == 0

        # Find words within Levenshtein distance of 3 from 'kitten'
        lev_func = FunctionCall(
            dialect, "levenshtein",
            Literal(dialect, "kitten"),
            Column(dialect, "word"),
        )
        where_pred = ComparisonPredicate(
            dialect, "<=",
            lev_func,
            Literal(dialect, 3),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "word")],
            from_=TableExpression(dialect, TABLE_NAME_ASYNC),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        words = [row["word"] for row in result.data]
        assert "kitten" in words
        assert "sitting" in words

    @pytest.mark.asyncio
    async def test_async_soundex(self, async_fuzzystrmatch_env):
        """Test soundex() encoding function."""
        backend, dialect = async_fuzzystrmatch_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Homophones should have the same soundex code
        func1 = FunctionCall(dialect, "soundex", Literal(dialect, "Robert")).as_("code")
        query = QueryExpression(dialect=dialect, select=[func1])
        sql, params = query.to_sql()
        result1 = await backend.execute(sql, params, options=opts)

        func2 = FunctionCall(dialect, "soundex", Literal(dialect, "Rupert")).as_("code")
        query = QueryExpression(dialect=dialect, select=[func2])
        sql, params = query.to_sql()
        result2 = await backend.execute(sql, params, options=opts)
        assert result1.data[0]["code"] == result2.data[0]["code"]

        # Note: Ashcraft and Ashcroft have the same soundex
        func3 = FunctionCall(dialect, "soundex", Literal(dialect, "Ashcraft")).as_("code")
        query = QueryExpression(dialect=dialect, select=[func3])
        sql, params = query.to_sql()
        result3 = await backend.execute(sql, params, options=opts)

        func4 = FunctionCall(dialect, "soundex", Literal(dialect, "Ashcroft")).as_("code")
        query = QueryExpression(dialect=dialect, select=[func4])
        sql, params = query.to_sql()
        result4 = await backend.execute(sql, params, options=opts)
        assert result3.data[0]["code"] == result4.data[0]["code"]

        # soundex code is 4 characters: letter + 3 digits
        func = FunctionCall(dialect, "soundex", Literal(dialect, "Hello")).as_("code")
        query = QueryExpression(dialect=dialect, select=[func])
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        code = result.data[0]["code"]
        assert len(code) == 4
        assert code[0].isalpha()
        assert code[1:].isdigit()

    @pytest.mark.asyncio
    async def test_async_difference(self, async_fuzzystrmatch_env):
        """Test difference() function (0-4 similarity based on soundex)."""
        backend, dialect = async_fuzzystrmatch_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Smith and Smythe should have high similarity
        func = FunctionCall(
            dialect, "difference",
            Literal(dialect, "Smith"),
            Literal(dialect, "Smythe"),
        ).as_("diff")
        query = QueryExpression(dialect=dialect, select=[func])
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data[0]["diff"] >= 3  # High similarity

        # Smith and Johnson should have low similarity
        func = FunctionCall(
            dialect, "difference",
            Literal(dialect, "Smith"),
            Literal(dialect, "Johnson"),
        ).as_("diff")
        query = QueryExpression(dialect=dialect, select=[func])
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data[0]["diff"] <= 2  # Low similarity

        # Find names that sound similar to 'Smith'
        diff_func = FunctionCall(
            dialect, "difference",
            Column(dialect, "name"),
            Literal(dialect, "Smith"),
        )
        where_pred = ComparisonPredicate(
            dialect, ">=",
            diff_func,
            Literal(dialect, 3),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "name")],
            from_=TableExpression(dialect, TABLE_NAME_DIFF_ASYNC),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        names = [row["name"] for row in result.data]
        assert "Smith" in names
        assert "Smythe" in names

    @pytest.mark.asyncio
    async def test_async_metaphone(self, async_fuzzystrmatch_env):
        """Test metaphone() function."""
        backend, dialect = async_fuzzystrmatch_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        func = FunctionCall(
            dialect, "metaphone",
            Literal(dialect, "hello"),
            Literal(dialect, 6),
        ).as_("meta")
        query = QueryExpression(dialect=dialect, select=[func])
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data[0]["meta"] is not None
        assert len(result.data[0]["meta"]) <= 6

        # Words with similar pronunciation should have similar metaphone codes
        func1 = FunctionCall(
            dialect, "metaphone",
            Literal(dialect, "phone"),
            Literal(dialect, 6),
        ).as_("meta")
        query = QueryExpression(dialect=dialect, select=[func1])
        sql, params = query.to_sql()
        result1 = await backend.execute(sql, params, options=opts)

        func2 = FunctionCall(
            dialect, "metaphone",
            Literal(dialect, "fone"),
            Literal(dialect, 6),
        ).as_("meta")
        query = QueryExpression(dialect=dialect, select=[func2])
        sql, params = query.to_sql()
        result2 = await backend.execute(sql, params, options=opts)
        assert result1.data[0]["meta"] == result2.data[0]["meta"]

    @pytest.mark.asyncio
    async def test_async_dmetaphone(self, async_fuzzystrmatch_env):
        """Test dmetaphone() function (double metaphone)."""
        backend, dialect = async_fuzzystrmatch_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        func = FunctionCall(
            dialect, "dmetaphone",
            Literal(dialect, "hello"),
        ).as_("dmeta")
        query = QueryExpression(dialect=dialect, select=[func])
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data[0]["dmeta"] is not None

        # dmetaphone provides better encoding for various languages
        func1 = FunctionCall(
            dialect, "dmetaphone",
            Literal(dialect, "Phillip"),
        ).as_("dmeta")
        query = QueryExpression(dialect=dialect, select=[func1])
        sql, params = query.to_sql()
        result1 = await backend.execute(sql, params, options=opts)

        func2 = FunctionCall(
            dialect, "dmetaphone",
            Literal(dialect, "Filip"),
        ).as_("dmeta")
        query = QueryExpression(dialect=dialect, select=[func2])
        sql, params = query.to_sql()
        result2 = await backend.execute(sql, params, options=opts)
        # Both should produce similar (or same) dmetaphone codes
        assert result1.data[0]["dmeta"] is not None
        assert result2.data[0]["dmeta"] is not None
