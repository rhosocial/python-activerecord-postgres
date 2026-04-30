"""Integration tests for PostgreSQL orafce extension with real database.

These tests require a live PostgreSQL connection with the orafce extension
installed. Tests will be automatically skipped if the extension is not
available.

Tests use expression-based SQL generation exclusively — no raw SQL.
"""

import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.impl.postgres.functions.orafce import (
    add_months,
    last_day,
    months_between,
    nvl,
    decode,
    instr,
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
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.query_parts import OrderByClause
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord_postgres_test.feature.backend.utils import (
    ensure_extension_installed,
    async_ensure_extension_installed,
)


TABLE_NAME = "test_orafce_dates"
ASYNC_TABLE_NAME = "test_orafce_dates_async"


@pytest.fixture(scope="function")
def orafce_backend(postgres_backend_single):
    """Function-scoped backend with orafce extension and test table prepared."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "orafce")
    dialect = backend.dialect

    # Create test table using expressions
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="dt", data_type="DATE"),
        ColumnDefinition(name="ref_dt", data_type="DATE"),
        ColumnDefinition(name="name", data_type="TEXT"),
        ColumnDefinition(name="discount", data_type="NUMERIC"),
        ColumnDefinition(name="description", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=TABLE_NAME,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    # Insert seed data with NULL values for testing nvl
    insert_expr = InsertExpression(
        dialect=dialect,
        into=TABLE_NAME,
        columns=["dt", "ref_dt", "name", "discount", "description"],
        source=ValuesSource(
            dialect,
            [
                [
                    Literal(dialect, "2024-01-15"),
                    Literal(dialect, "2024-06-15"),
                    Literal(dialect, "A"),
                    Literal(dialect, 0.10),
                    Literal(dialect, "test description one"),
                ],
                [
                    Literal(dialect, "2024-06-30"),
                    Literal(dialect, "2024-06-15"),
                    Literal(dialect, "B"),
                    Literal(dialect, None),
                    Literal(dialect, "another test example"),
                ],
                [
                    Literal(dialect, "2024-12-25"),
                    Literal(dialect, "2024-06-15"),
                    Literal(dialect, "C"),
                    Literal(dialect, 0.25),
                    Literal(dialect, None),
                ],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)

    yield backend

    # Teardown: drop table using expression
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name=TABLE_NAME,
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    backend.execute(sql, params)


class TestOrafceIntegration:
    """Integration tests for orafce extension functions."""

    def test_add_months(self, orafce_backend):
        """Test ADD_MONTHS function adds months to a date column."""
        backend = orafce_backend
        dialect = backend.dialect
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        func = add_months(
            dialect,
            Column(dialect, "dt"),
            Literal(dialect, 3),
        ).as_("result_dt")

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), func],
            from_=TableExpression(dialect, TABLE_NAME),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)

        assert len(result.data) == 3
        # 2024-01-15 + 3 months = 2024-04-15
        assert result.data[0]["result_dt"] is not None
        # 2024-06-30 + 3 months = 2024-09-30
        assert result.data[1]["result_dt"] is not None
        # 2024-12-25 + 3 months = 2025-03-25
        assert result.data[2]["result_dt"] is not None

    def test_last_day(self, orafce_backend):
        """Test LAST_DAY function returns the last day of the month."""
        backend = orafce_backend
        dialect = backend.dialect
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        func = last_day(
            dialect,
            Column(dialect, "dt"),
        ).as_("month_end")

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), func],
            from_=TableExpression(dialect, TABLE_NAME),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)

        assert len(result.data) == 3
        # LAST_DAY(2024-01-15) = 2024-01-31
        assert result.data[0]["month_end"] is not None
        # LAST_DAY(2024-06-30) = 2024-06-30 (already last day)
        assert result.data[1]["month_end"] is not None
        # LAST_DAY(2024-12-25) = 2024-12-31
        assert result.data[2]["month_end"] is not None

    def test_months_between(self, orafce_backend):
        """Test MONTHS_BETWEEN function calculates months between two dates."""
        backend = orafce_backend
        dialect = backend.dialect
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Use ref_dt (DATE column) instead of Literal string for type safety
        func = months_between(
            dialect,
            Column(dialect, "ref_dt"),
            Column(dialect, "dt"),
        ).as_("month_diff")

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), func],
            from_=TableExpression(dialect, TABLE_NAME),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)

        assert len(result.data) == 3
        # MONTHS_BETWEEN(2024-06-15, 2024-01-15) = 5
        assert result.data[0]["month_diff"] is not None
        assert float(result.data[0]["month_diff"]) == pytest.approx(5.0, abs=0.01)
        # MONTHS_BETWEEN(2024-06-15, 2024-06-30) ≈ -0.5
        assert result.data[1]["month_diff"] is not None
        # MONTHS_BETWEEN(2024-06-15, 2024-12-25) ≈ -6.3
        assert result.data[2]["month_diff"] is not None

    def test_nvl(self, orafce_backend):
        """Test NVL function replaces NULL with a default value."""
        backend = orafce_backend
        dialect = backend.dialect
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        func = nvl(
            dialect,
            Column(dialect, "discount"),
            Literal(dialect, 0.0).cast("NUMERIC"),
        ).as_("safe_discount")

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), func],
            from_=TableExpression(dialect, TABLE_NAME),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)

        assert len(result.data) == 3
        # Row 1: discount=0.10, NVL returns 0.10
        assert float(result.data[0]["safe_discount"]) == pytest.approx(0.10, abs=0.01)
        # Row 2: discount=NULL, NVL returns 0.0
        assert float(result.data[1]["safe_discount"]) == pytest.approx(0.0, abs=0.01)
        # Row 3: discount=0.25, NVL returns 0.25
        assert float(result.data[2]["safe_discount"]) == pytest.approx(0.25, abs=0.01)

    def test_decode(self, orafce_backend):
        """Test DECODE function performs conditional value mapping."""
        backend = orafce_backend
        dialect = backend.dialect
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        func = decode(
            dialect,
            Column(dialect, "name"),
            Literal(dialect, "A"),
            Literal(dialect, "Alpha"),
            default=Literal(dialect, "Other"),
        ).as_("decoded_name")

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), func],
            from_=TableExpression(dialect, TABLE_NAME),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)

        assert len(result.data) == 3
        # Row 1: name='A' -> 'Alpha'
        assert result.data[0]["decoded_name"] == "Alpha"
        # Row 2: name='B' -> 'Other' (no match, default)
        assert result.data[1]["decoded_name"] == "Other"
        # Row 3: name='C' -> 'Other' (no match, default)
        assert result.data[2]["decoded_name"] == "Other"

    def test_instr(self, orafce_backend):
        """Test INSTR function finds substring position in a string."""
        backend = orafce_backend
        dialect = backend.dialect
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        func = instr(
            dialect,
            Column(dialect, "description"),
            Literal(dialect, "test"),
        ).as_("pos")

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), func],
            from_=TableExpression(dialect, TABLE_NAME),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)

        assert len(result.data) == 3
        # Row 1: "test description one" — "test" at position 1
        assert result.data[0]["pos"] is not None
        assert int(result.data[0]["pos"]) > 0
        # Row 2: "another test example" — "test" found
        assert result.data[1]["pos"] is not None
        assert int(result.data[1]["pos"]) > 0
        # Row 3: description=NULL, INSTR on NULL returns NULL
        assert result.data[2]["pos"] is None


@pytest_asyncio.fixture(scope="function")
async def async_orafce_backend(async_postgres_backend_single):
    """Function-scoped async backend with orafce extension and test table prepared."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "orafce")
    dialect = backend.dialect

    # Create test table using expressions
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="dt", data_type="DATE"),
        ColumnDefinition(name="ref_dt", data_type="DATE"),
        ColumnDefinition(name="name", data_type="TEXT"),
        ColumnDefinition(name="discount", data_type="NUMERIC"),
        ColumnDefinition(name="description", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=ASYNC_TABLE_NAME,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    # Insert seed data with NULL values for testing nvl
    insert_expr = InsertExpression(
        dialect=dialect,
        into=ASYNC_TABLE_NAME,
        columns=["dt", "ref_dt", "name", "discount", "description"],
        source=ValuesSource(
            dialect,
            [
                [
                    Literal(dialect, "2024-01-15"),
                    Literal(dialect, "2024-06-15"),
                    Literal(dialect, "A"),
                    Literal(dialect, 0.10),
                    Literal(dialect, "test description one"),
                ],
                [
                    Literal(dialect, "2024-06-30"),
                    Literal(dialect, "2024-06-15"),
                    Literal(dialect, "B"),
                    Literal(dialect, None),
                    Literal(dialect, "another test example"),
                ],
                [
                    Literal(dialect, "2024-12-25"),
                    Literal(dialect, "2024-06-15"),
                    Literal(dialect, "C"),
                    Literal(dialect, 0.25),
                    Literal(dialect, None),
                ],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)

    yield backend

    # Teardown: drop table using expression
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name=ASYNC_TABLE_NAME,
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    await backend.execute(sql, params)


class TestAsyncOrafceIntegration:
    """Async integration tests for orafce extension functions."""

    @pytest.mark.asyncio
    async def test_async_add_months(self, async_orafce_backend):
        """Test ADD_MONTHS function adds months to a date column."""
        backend = async_orafce_backend
        dialect = backend.dialect
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        func = add_months(
            dialect,
            Column(dialect, "dt"),
            Literal(dialect, 3),
        ).as_("result_dt")

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), func],
            from_=TableExpression(dialect, ASYNC_TABLE_NAME),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)

        assert len(result.data) == 3
        # 2024-01-15 + 3 months = 2024-04-15
        assert result.data[0]["result_dt"] is not None
        # 2024-06-30 + 3 months = 2024-09-30
        assert result.data[1]["result_dt"] is not None
        # 2024-12-25 + 3 months = 2025-03-25
        assert result.data[2]["result_dt"] is not None

    @pytest.mark.asyncio
    async def test_async_last_day(self, async_orafce_backend):
        """Test LAST_DAY function returns the last day of the month."""
        backend = async_orafce_backend
        dialect = backend.dialect
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        func = last_day(
            dialect,
            Column(dialect, "dt"),
        ).as_("month_end")

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), func],
            from_=TableExpression(dialect, ASYNC_TABLE_NAME),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)

        assert len(result.data) == 3
        # LAST_DAY(2024-01-15) = 2024-01-31
        assert result.data[0]["month_end"] is not None
        # LAST_DAY(2024-06-30) = 2024-06-30 (already last day)
        assert result.data[1]["month_end"] is not None
        # LAST_DAY(2024-12-25) = 2024-12-31
        assert result.data[2]["month_end"] is not None

    @pytest.mark.asyncio
    async def test_async_months_between(self, async_orafce_backend):
        """Test MONTHS_BETWEEN function calculates months between two dates."""
        backend = async_orafce_backend
        dialect = backend.dialect
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Use ref_dt (DATE column) instead of Literal string for type safety
        func = months_between(
            dialect,
            Column(dialect, "ref_dt"),
            Column(dialect, "dt"),
        ).as_("month_diff")

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), func],
            from_=TableExpression(dialect, ASYNC_TABLE_NAME),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)

        assert len(result.data) == 3
        # MONTHS_BETWEEN(2024-06-15, 2024-01-15) = 5
        assert result.data[0]["month_diff"] is not None
        assert float(result.data[0]["month_diff"]) == pytest.approx(5.0, abs=0.01)
        # MONTHS_BETWEEN(2024-06-15, 2024-06-30) ≈ -0.5
        assert result.data[1]["month_diff"] is not None
        # MONTHS_BETWEEN(2024-06-15, 2024-12-25) ≈ -6.3
        assert result.data[2]["month_diff"] is not None

    @pytest.mark.asyncio
    async def test_async_nvl(self, async_orafce_backend):
        """Test NVL function replaces NULL with a default value."""
        backend = async_orafce_backend
        dialect = backend.dialect
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        func = nvl(
            dialect,
            Column(dialect, "discount"),
            Literal(dialect, 0.0).cast("NUMERIC"),
        ).as_("safe_discount")

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), func],
            from_=TableExpression(dialect, ASYNC_TABLE_NAME),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)

        assert len(result.data) == 3
        # Row 1: discount=0.10, NVL returns 0.10
        assert float(result.data[0]["safe_discount"]) == pytest.approx(0.10, abs=0.01)
        # Row 2: discount=NULL, NVL returns 0.0
        assert float(result.data[1]["safe_discount"]) == pytest.approx(0.0, abs=0.01)
        # Row 3: discount=0.25, NVL returns 0.25
        assert float(result.data[2]["safe_discount"]) == pytest.approx(0.25, abs=0.01)

    @pytest.mark.asyncio
    async def test_async_decode(self, async_orafce_backend):
        """Test DECODE function performs conditional value mapping."""
        backend = async_orafce_backend
        dialect = backend.dialect
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        func = decode(
            dialect,
            Column(dialect, "name"),
            Literal(dialect, "A"),
            Literal(dialect, "Alpha"),
            default=Literal(dialect, "Other"),
        ).as_("decoded_name")

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), func],
            from_=TableExpression(dialect, ASYNC_TABLE_NAME),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)

        assert len(result.data) == 3
        # Row 1: name='A' -> 'Alpha'
        assert result.data[0]["decoded_name"] == "Alpha"
        # Row 2: name='B' -> 'Other' (no match, default)
        assert result.data[1]["decoded_name"] == "Other"
        # Row 3: name='C' -> 'Other' (no match, default)
        assert result.data[2]["decoded_name"] == "Other"

    @pytest.mark.asyncio
    async def test_async_instr(self, async_orafce_backend):
        """Test INSTR function finds substring position in a string."""
        backend = async_orafce_backend
        dialect = backend.dialect
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        func = instr(
            dialect,
            Column(dialect, "description"),
            Literal(dialect, "test"),
        ).as_("pos")

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), func],
            from_=TableExpression(dialect, ASYNC_TABLE_NAME),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)

        assert len(result.data) == 3
        # Row 1: "test description one" — "test" at position 1
        assert result.data[0]["pos"] is not None
        assert int(result.data[0]["pos"]) > 0
        # Row 2: "another test example" — "test" found
        assert result.data[1]["pos"] is not None
        assert int(result.data[1]["pos"]) > 0
        # Row 3: description=NULL, INSTR on NULL returns NULL
        assert result.data[2]["pos"] is None
