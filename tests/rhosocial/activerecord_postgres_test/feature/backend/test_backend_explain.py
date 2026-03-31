# tests/rhosocial/activerecord_postgres_test/feature/backend/test_backend_explain.py
"""
Integration tests for PostgresBackend.explain() and AsyncPostgresBackend.explain().

These tests require a real PostgreSQL connection configured via postgres_scenarios.yaml.
The tests create temporary tables, run EXPLAIN, and verify the typed result objects.
"""
import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.explain import (
    SyncExplainBackendProtocol,
    AsyncExplainBackendProtocol,
)
from rhosocial.activerecord.backend.expression import RawSQLExpression
from rhosocial.activerecord.backend.expression.statements import ExplainOptions
from rhosocial.activerecord.backend.impl.postgres import (
    PostgresExplainResult,
    PostgresExplainPlanLine,
)


# ---------------------------------------------------------------------------
# Schema helpers (PostgreSQL syntax)
# ---------------------------------------------------------------------------

_SETUP_SQL = """
    DROP TABLE IF EXISTS explain_order_items CASCADE;
    DROP TABLE IF EXISTS explain_orders CASCADE;

    CREATE TABLE explain_orders (
        id      SERIAL       PRIMARY KEY,
        status  VARCHAR(20)  NOT NULL,
        amount  NUMERIC(10,2)
    );
    CREATE INDEX idx_orders_status ON explain_orders(status);

    CREATE TABLE explain_order_items (
        id       SERIAL      PRIMARY KEY,
        order_id INTEGER     NOT NULL,
        sku      VARCHAR(50) NOT NULL,
        qty      INTEGER     NOT NULL DEFAULT 1
    );
    CREATE INDEX idx_items_order_id_sku ON explain_order_items(order_id, sku);

    INSERT INTO explain_orders (status, amount) VALUES
        ('pending', 10.00), ('pending', 20.00),
        ('shipped', 30.00), ('delivered', 40.00);

    INSERT INTO explain_order_items (order_id, sku, qty) VALUES
        (1, 'A001', 1), (1, 'A002', 2),
        (2, 'B001', 1), (3, 'A001', 3);
"""

_CLEANUP_SQL = """
    DROP TABLE IF EXISTS explain_order_items CASCADE;
    DROP TABLE IF EXISTS explain_orders CASCADE;
"""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def indexed_backend(postgres_backend):
    """Sync backend with test tables and indexes."""
    postgres_backend.executescript(_SETUP_SQL)
    yield postgres_backend
    try:
        postgres_backend.executescript(_CLEANUP_SQL)
    except Exception:
        pass


@pytest_asyncio.fixture(scope="function")
async def async_indexed_backend(async_postgres_backend):
    """Async backend with test tables and indexes."""
    await async_postgres_backend.executescript(_SETUP_SQL)
    yield async_postgres_backend
    try:
        await async_postgres_backend.executescript(_CLEANUP_SQL)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Protocol checking
# ---------------------------------------------------------------------------

class TestExplainProtocol:
    def test_sync_backend_implements_protocol(self, postgres_backend):
        assert isinstance(postgres_backend, SyncExplainBackendProtocol)

    @pytest.mark.asyncio
    async def test_async_backend_implements_protocol(self, async_postgres_backend):
        assert isinstance(async_postgres_backend, AsyncExplainBackendProtocol)


# ---------------------------------------------------------------------------
# Sync explain – basic structure
# ---------------------------------------------------------------------------

class TestSyncExplainBasic:
    def test_explain_returns_postgres_explain_result(self, indexed_backend):
        dialect = indexed_backend.dialect
        expr = RawSQLExpression(dialect, "SELECT * FROM explain_orders")
        result = indexed_backend.explain(expr)
        assert isinstance(result, PostgresExplainResult)

    def test_result_has_rows(self, indexed_backend):
        dialect = indexed_backend.dialect
        expr = RawSQLExpression(dialect, "SELECT * FROM explain_orders")
        result = indexed_backend.explain(expr)
        assert len(result.rows) > 0

    def test_result_row_type(self, indexed_backend):
        dialect = indexed_backend.dialect
        expr = RawSQLExpression(dialect, "SELECT * FROM explain_orders")
        result = indexed_backend.explain(expr)
        for row in result.rows:
            assert isinstance(row, PostgresExplainPlanLine)
            assert isinstance(row.line, str)
            assert len(row.line) > 0

    def test_result_sql_starts_with_explain(self, indexed_backend):
        dialect = indexed_backend.dialect
        expr = RawSQLExpression(dialect, "SELECT * FROM explain_orders")
        result = indexed_backend.explain(expr)
        assert result.sql.upper().startswith("EXPLAIN")
        assert "explain_orders" in result.sql.lower()

    def test_result_has_duration(self, indexed_backend):
        dialect = indexed_backend.dialect
        expr = RawSQLExpression(dialect, "SELECT * FROM explain_orders")
        result = indexed_backend.explain(expr)
        assert result.duration >= 0.0

    def test_result_has_raw_rows(self, indexed_backend):
        dialect = indexed_backend.dialect
        expr = RawSQLExpression(dialect, "SELECT * FROM explain_orders")
        result = indexed_backend.explain(expr)
        assert isinstance(result.raw_rows, list)
        assert len(result.raw_rows) == len(result.rows)
        # Each raw row must have the 'QUERY PLAN' key
        for raw in result.raw_rows:
            assert "QUERY PLAN" in raw


# ---------------------------------------------------------------------------
# Sync explain – index usage analysis
# ---------------------------------------------------------------------------

class TestSyncExplainIndexAnalysis:
    def test_full_scan_detection(self, indexed_backend):
        """SELECT * FROM table without WHERE → Seq Scan → full_scan."""
        dialect = indexed_backend.dialect
        # ANALYZE forces actual execution so the planner won't cheat
        result = indexed_backend.explain(
            RawSQLExpression(dialect, "SELECT * FROM explain_orders")
        )
        assert result.analyze_index_usage() == "full_scan"
        assert result.is_full_scan is True
        assert result.is_index_used is False
        assert result.is_covering_index is False

    def test_index_used_detection(self, indexed_backend):
        """SELECT * … WHERE indexed_col = ? → Index Scan (with heap access)."""
        dialect = indexed_backend.dialect
        result = indexed_backend.explain(
            RawSQLExpression(dialect, "SELECT * FROM explain_orders WHERE status = 'pending'")
        )
        usage = result.analyze_index_usage()
        # Small table may still use Seq Scan; if it uses an index, verify the flag
        if usage != "full_scan":
            assert result.is_index_used is True
            assert result.is_full_scan is False

    def test_covering_index_detection(self, indexed_backend):
        """SELECT indexed_cols WHERE indexed_col = ? → Index Only Scan."""
        dialect = indexed_backend.dialect
        result = indexed_backend.explain(
            RawSQLExpression(
                dialect,
                "SELECT order_id, sku FROM explain_order_items WHERE order_id = 1"
            )
        )
        usage = result.analyze_index_usage()
        # PostgreSQL may choose Index Only Scan for covered queries
        assert usage in ("covering_index", "index_with_lookup", "full_scan")
        if usage == "covering_index":
            assert result.is_covering_index is True

    def test_plan_lines_contain_text(self, indexed_backend):
        """Plan lines should contain meaningful plan text."""
        dialect = indexed_backend.dialect
        result = indexed_backend.explain(
            RawSQLExpression(dialect, "SELECT * FROM explain_orders")
        )
        combined = " ".join(r.line for r in result.rows)
        # Should mention the table name or a scan type
        assert any(kw in combined.upper() for kw in ("SCAN", "EXPLAIN_ORDERS", "SEQ"))


# ---------------------------------------------------------------------------
# Sync explain – ANALYZE option
# ---------------------------------------------------------------------------

class TestSyncExplainAnalyze:
    def test_explain_analyze_returns_result(self, indexed_backend):
        """EXPLAIN (ANALYZE) executes the query and returns runtime stats."""
        dialect = indexed_backend.dialect
        if not dialect.supports_explain_analyze():
            pytest.skip("Dialect does not support EXPLAIN ANALYZE")
        opts = ExplainOptions(analyze=True)
        result = indexed_backend.explain(
            RawSQLExpression(dialect, "SELECT * FROM explain_orders"),
            opts,
        )
        assert isinstance(result, PostgresExplainResult)
        assert len(result.rows) > 0
        assert "ANALYZE" in result.sql.upper()
        # ANALYZE output typically includes "actual time"
        combined = " ".join(r.line for r in result.rows).lower()
        assert "actual" in combined or "time" in combined

    def test_explain_analyze_sql_prefix(self, indexed_backend):
        """Verify the generated SQL uses bracket syntax: EXPLAIN (ANALYZE)."""
        dialect = indexed_backend.dialect
        if not dialect.supports_explain_analyze():
            pytest.skip("Dialect does not support EXPLAIN ANALYZE")
        opts = ExplainOptions(analyze=True)
        result = indexed_backend.explain(
            RawSQLExpression(dialect, "SELECT 1"),
            opts,
        )
        assert result.sql.upper().startswith("EXPLAIN (ANALYZE)")


# ---------------------------------------------------------------------------
# Async explain – mirror of sync tests
# ---------------------------------------------------------------------------

class TestAsyncExplainBasic:
    @pytest.mark.asyncio
    async def test_explain_returns_postgres_explain_result(self, async_indexed_backend):
        dialect = async_indexed_backend.dialect
        expr = RawSQLExpression(dialect, "SELECT * FROM explain_orders")
        result = await async_indexed_backend.explain(expr)
        assert isinstance(result, PostgresExplainResult)

    @pytest.mark.asyncio
    async def test_result_has_rows(self, async_indexed_backend):
        dialect = async_indexed_backend.dialect
        expr = RawSQLExpression(dialect, "SELECT * FROM explain_orders")
        result = await async_indexed_backend.explain(expr)
        assert len(result.rows) > 0

    @pytest.mark.asyncio
    async def test_result_sql_and_duration(self, async_indexed_backend):
        dialect = async_indexed_backend.dialect
        expr = RawSQLExpression(dialect, "SELECT * FROM explain_orders")
        result = await async_indexed_backend.explain(expr)
        assert result.sql.upper().startswith("EXPLAIN")
        assert result.duration >= 0.0

    @pytest.mark.asyncio
    async def test_full_scan_detection(self, async_indexed_backend):
        dialect = async_indexed_backend.dialect
        result = await async_indexed_backend.explain(
            RawSQLExpression(dialect, "SELECT * FROM explain_orders")
        )
        assert result.is_full_scan is True

    @pytest.mark.asyncio
    async def test_raw_rows_have_query_plan_key(self, async_indexed_backend):
        dialect = async_indexed_backend.dialect
        result = await async_indexed_backend.explain(
            RawSQLExpression(dialect, "SELECT * FROM explain_orders")
        )
        for raw in result.raw_rows:
            assert "QUERY PLAN" in raw
