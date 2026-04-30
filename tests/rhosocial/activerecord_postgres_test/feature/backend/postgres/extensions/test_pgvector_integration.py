"""Integration tests for the pgvector extension.

These tests require a PostgreSQL database with the pgvector extension installed.
Tests will be automatically skipped if the extension is not available.

Vector literal values use Literal.cast("vector") for inline embedding because
psycopg does not natively support the vector type for bound parameters.
pgvector distance operators (<->, <=>, <#>) still require RawSQLExpression
because ComparisonPredicate does not yet support .as_() alias.
"""

import pytest
import pytest_asyncio

from rhosocial.activerecord_postgres_test.feature.backend.utils import (
    ensure_extension_installed,
    async_ensure_extension_installed,
)
from rhosocial.activerecord.backend.errors import DatabaseError
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
    UpdateExpression,
    RawSQLExpression,
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.query_parts import OrderByClause
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


# --- Helper: common VECTOR(3) column definitions ---


def _id_column():
    return ColumnDefinition(
        name="id",
        data_type="SERIAL",
        constraints=[
            ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
        ],
    )


def _embedding_column(dim=3):
    return ColumnDefinition(name="embedding", data_type=f"VECTOR({dim})")


# --- Sync fixture and tests ---


@pytest.fixture
def vector_env(postgres_backend_single):
    """Independent test environment for pgvector extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "vector")
    dialect = backend.dialect

    table_names = [
        "test_vector_items",
        "test_vector_l2",
        "test_vector_cosine",
        "test_vector_ip",
    ]

    # Clean up any leftover tables from prior runs
    for table_name in table_names:
        drop_expr = DropTableExpression(
            dialect=dialect,
            table_name=table_name,
            if_exists=True,
        )
        sql, params = drop_expr.to_sql()
        backend.execute(sql, params)

    # --- test_vector_items ---
    create_items = CreateTableExpression(
        dialect=dialect,
        table_name="test_vector_items",
        columns=[_id_column(), _embedding_column()],
    )
    sql, params = create_items.to_sql()
    backend.execute(sql, params)

    insert_items = InsertExpression(
        dialect=dialect,
        into="test_vector_items",
        columns=["embedding"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "[1.0, 2.0, 3.0]").cast("vector")],
                [Literal(dialect, "[4.0, 5.0, 6.0]").cast("vector")],
            ],
        ),
    )
    sql, params = insert_items.to_sql()
    backend.execute(sql, params)

    # --- test_vector_l2 ---
    create_l2 = CreateTableExpression(
        dialect=dialect,
        table_name="test_vector_l2",
        columns=[_id_column(), _embedding_column()],
    )
    sql, params = create_l2.to_sql()
    backend.execute(sql, params)

    insert_l2 = InsertExpression(
        dialect=dialect,
        into="test_vector_l2",
        columns=["embedding"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "[1.0, 0.0, 0.0]").cast("vector")],
                [Literal(dialect, "[0.0, 1.0, 0.0]").cast("vector")],
                [Literal(dialect, "[0.0, 0.0, 1.0]").cast("vector")],
            ],
        ),
    )
    sql, params = insert_l2.to_sql()
    backend.execute(sql, params)

    # --- test_vector_cosine ---
    create_cosine = CreateTableExpression(
        dialect=dialect,
        table_name="test_vector_cosine",
        columns=[_id_column(), _embedding_column()],
    )
    sql, params = create_cosine.to_sql()
    backend.execute(sql, params)

    insert_cosine = InsertExpression(
        dialect=dialect,
        into="test_vector_cosine",
        columns=["embedding"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "[1.0, 0.0, 0.0]").cast("vector")],
                [Literal(dialect, "[1.0, 1.0, 0.0]").cast("vector")],
                [Literal(dialect, "[0.0, 0.0, 1.0]").cast("vector")],
            ],
        ),
    )
    sql, params = insert_cosine.to_sql()
    backend.execute(sql, params)

    # --- test_vector_ip ---
    create_ip = CreateTableExpression(
        dialect=dialect,
        table_name="test_vector_ip",
        columns=[_id_column(), _embedding_column()],
    )
    sql, params = create_ip.to_sql()
    backend.execute(sql, params)

    insert_ip = InsertExpression(
        dialect=dialect,
        into="test_vector_ip",
        columns=["embedding"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "[1.0, 2.0, 3.0]").cast("vector")],
                [Literal(dialect, "[-1.0, 0.0, 0.0]").cast("vector")],
            ],
        ),
    )
    sql, params = insert_ip.to_sql()
    backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop tables
    for table_name in [
        "test_vector_items",
        "test_vector_l2",
        "test_vector_cosine",
        "test_vector_ip",
    ]:
        drop_expr = DropTableExpression(
            dialect=dialect,
            table_name=table_name,
            if_exists=True,
        )
        sql, params = drop_expr.to_sql()
        backend.execute(sql, params)


class TestPgvectorIntegration:
    """Integration tests for pgvector vector similarity search."""

    def test_vector_type_crud(self, vector_env):
        """Test querying vector column data."""
        backend, dialect = vector_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "embedding")],
            from_=TableExpression(dialect, "test_vector_items"),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 2
        assert result.data[0]['embedding'] is not None
        assert result.data[1]['embedding'] is not None

    def test_l2_distance(self, vector_env):
        """Test L2 (Euclidean) distance operator <->."""
        backend, dialect = vector_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        distance_expr = RawSQLExpression(
            dialect, "embedding <-> '[1.0, 0.0, 0.0]' AS distance"
        )

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), distance_expr],
            from_=TableExpression(dialect, "test_vector_l2"),
            order_by=OrderByClause(
                dialect,
                # TODO: Replace with ComparisonPredicate when it supports .as_() alias
                [RawSQLExpression(dialect, "embedding <-> '[1.0, 0.0, 0.0]'")],
            ),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 3
        # [1,0,0] distance from itself is 0
        assert float(result.data[0]['distance']) == pytest.approx(0.0, abs=1e-6)
        # [0,1,0] and [0,0,1] both have distance sqrt(2) from [1,0,0]
        assert float(result.data[1]['distance']) == pytest.approx(1.41421356, abs=1e-4)

    def test_cosine_distance(self, vector_env):
        """Test cosine distance operator <=>."""
        backend, dialect = vector_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        distance_expr = RawSQLExpression(
            dialect, "embedding <=> '[1.0, 0.0, 0.0]' AS distance"
        )

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), distance_expr],
            from_=TableExpression(dialect, "test_vector_cosine"),
            order_by=OrderByClause(
                dialect,
                # TODO: Replace with ComparisonPredicate when it supports .as_() alias
                [RawSQLExpression(dialect, "embedding <=> '[1.0, 0.0, 0.0]'")],
            ),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 3
        # Cosine distance of identical vectors is 0
        assert float(result.data[0]['distance']) == pytest.approx(0.0, abs=1e-6)
        # [1,1,0] cosine distance from [1,0,0] = 1 - 1/sqrt(2)
        assert float(result.data[1]['distance']) == pytest.approx(0.29289, abs=1e-3)

    def test_inner_product(self, vector_env):
        """Test inner product operator <#>."""
        backend, dialect = vector_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        ip_expr = RawSQLExpression(
            dialect, "embedding <#> '[1.0, 0.0, 0.0]' AS ip"
        )

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), ip_expr],
            from_=TableExpression(dialect, "test_vector_ip"),
            order_by=OrderByClause(
                dialect,
                # TODO: Replace with ComparisonPredicate when it supports .as_() alias
                [RawSQLExpression(dialect, "embedding <#> '[1.0, 0.0, 0.0]'")],
            ),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 2
        # <#> returns negative inner product
        # [-1,0,0] . [1,0,0] = -1, <#> returns 1 (smallest first)
        assert float(result.data[0]['ip']) == pytest.approx(-1.0, abs=1e-4)

    def test_vector_update(self, vector_env):
        """Test updating vector values."""
        backend, dialect = vector_env

        # Create temporary table
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="test_vector_update",
            columns=[_id_column(), _embedding_column()],
        )
        sql, params = create_expr.to_sql()
        backend.execute(sql, params)

        try:
            # Insert initial vector
            insert_expr = InsertExpression(
                dialect=dialect,
                into="test_vector_update",
                columns=["embedding"],
                source=ValuesSource(
                    dialect,
                    [[Literal(dialect, "[1.0, 2.0, 3.0]").cast("vector")]],
                ),
            )
            sql, params = insert_expr.to_sql()
            backend.execute(sql, params)

            # Update the vector
            update_expr = UpdateExpression(
                dialect=dialect,
                table="test_vector_update",
                assignments={"embedding": Literal(dialect, "[4.0, 5.0, 6.0]").cast("vector")},
                where=Column(dialect, "id") == Literal(dialect, 1),
            )
            sql, params = update_expr.to_sql()
            backend.execute(sql, params)

            # Verify update
            opts = ExecutionOptions(stmt_type=StatementType.DQL)
            query = QueryExpression(
                dialect=dialect,
                select=[Column(dialect, "embedding")],
                from_=TableExpression(dialect, "test_vector_update"),
                where=Column(dialect, "id") == Literal(dialect, 1),
            )
            sql, params = query.to_sql()
            result = backend.execute(sql, params, options=opts)
            assert result.data is not None
            assert result.data[0]['embedding'] is not None
        finally:
            drop_expr = DropTableExpression(
                dialect=dialect,
                table_name="test_vector_update",
                if_exists=True,
            )
            sql, params = drop_expr.to_sql()
            backend.execute(sql, params)

    def test_ivfflat_index(self, vector_env):
        """Test creating an IVFFlat index on a vector column."""
        backend, dialect = vector_env

        # Create table for IVFFlat index test
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="test_vector_ivfflat",
            columns=[_id_column(), _embedding_column()],
        )
        sql, params = create_expr.to_sql()
        backend.execute(sql, params)

        try:
            # Insert 10 rows of vector data
            rows = [
                [Literal(dialect, f"[{i}.0, {i+1}.0, {i+2}.0]").cast("vector")]
                for i in range(10)
            ]
            insert_expr = InsertExpression(
                dialect=dialect,
                into="test_vector_ivfflat",
                columns=["embedding"],
                source=ValuesSource(dialect, rows),
            )
            sql, params = insert_expr.to_sql()
            backend.execute(sql, params)

            # CREATE INDEX with opclass and WITH parameters is not supported
            # by the expression system, so raw SQL is used here.
            backend.execute(
                "CREATE INDEX idx_test_ivfflat ON test_vector_ivfflat "
                "USING ivfflat (embedding vector_cosine_ops) WITH (lists = 5)"
            )

            # Verify index exists
            opts = ExecutionOptions(stmt_type=StatementType.DQL)
            query = QueryExpression(
                dialect=dialect,
                select=[Column(dialect, "indexname")],
                from_=TableExpression(dialect, "pg_indexes"),
                where=Column(dialect, "tablename") == Literal(dialect, "test_vector_ivfflat"),
            )
            sql, params = query.to_sql()
            result = backend.execute(sql, params, options=opts)
            assert result.data is not None
            index_names = [r['indexname'] for r in result.data]
            assert "idx_test_ivfflat" in index_names
        finally:
            drop_expr = DropTableExpression(
                dialect=dialect,
                table_name="test_vector_ivfflat",
                if_exists=True,
            )
            sql, params = drop_expr.to_sql()
            backend.execute(sql, params)

    def test_hnsw_index(self, vector_env):
        """Test creating an HNSW index on a vector column."""
        backend, dialect = vector_env

        # Check HNSW feature support
        if not dialect.check_extension_feature("vector", "hnsw_index"):
            pytest.skip("Extension 'vector' feature 'hnsw_index' not supported")

        # Create table for HNSW index test
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="test_vector_hnsw",
            columns=[_id_column(), _embedding_column()],
        )
        sql, params = create_expr.to_sql()
        backend.execute(sql, params)

        try:
            # Insert 10 rows of vector data
            rows = [
                [Literal(dialect, f"[{i}.0, {i+1}.0, {i+2}.0]").cast("vector")]
                for i in range(10)
            ]
            insert_expr = InsertExpression(
                dialect=dialect,
                into="test_vector_hnsw",
                columns=["embedding"],
                source=ValuesSource(dialect, rows),
            )
            sql, params = insert_expr.to_sql()
            backend.execute(sql, params)

            # CREATE INDEX with opclass is not supported by the expression
            # system, so raw SQL is used here.
            backend.execute(
                "CREATE INDEX idx_test_hnsw ON test_vector_hnsw "
                "USING hnsw (embedding vector_cosine_ops)"
            )

            # Verify index exists
            opts = ExecutionOptions(stmt_type=StatementType.DQL)
            query = QueryExpression(
                dialect=dialect,
                select=[Column(dialect, "indexname")],
                from_=TableExpression(dialect, "pg_indexes"),
                where=Column(dialect, "tablename") == Literal(dialect, "test_vector_hnsw"),
            )
            sql, params = query.to_sql()
            result = backend.execute(sql, params, options=opts)
            assert result.data is not None
            index_names = [r['indexname'] for r in result.data]
            assert "idx_test_hnsw" in index_names
        finally:
            drop_expr = DropTableExpression(
                dialect=dialect,
                table_name="test_vector_hnsw",
                if_exists=True,
            )
            sql, params = drop_expr.to_sql()
            backend.execute(sql, params)

    def test_vector_dimension_constraint(self, vector_env):
        """Test that vector dimension constraint is enforced."""
        backend, dialect = vector_env

        # Create table with VECTOR(3) dimension constraint
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="test_vector_dim",
            columns=[_id_column(), _embedding_column()],
        )
        sql, params = create_expr.to_sql()
        backend.execute(sql, params)

        try:
            # Inserting a vector with wrong dimension should fail
            insert_expr = InsertExpression(
                dialect=dialect,
                into="test_vector_dim",
                columns=["embedding"],
                source=ValuesSource(
                    dialect,
                    [[Literal(dialect, "[1.0, 2.0]").cast("vector")]],
                ),
            )
            sql, params = insert_expr.to_sql()
            with pytest.raises(DatabaseError):
                backend.execute(sql, params)
        finally:
            drop_expr = DropTableExpression(
                dialect=dialect,
                table_name="test_vector_dim",
                if_exists=True,
            )
            sql, params = drop_expr.to_sql()
            backend.execute(sql, params)


# --- Async fixture and tests ---


@pytest_asyncio.fixture
async def async_vector_env(async_postgres_backend_single):
    """Independent async test environment for pgvector extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "vector")
    dialect = backend.dialect

    async_table_names = [
        "test_vector_items_async",
        "test_vector_l2_async",
        "test_vector_cosine_async",
        "test_vector_ip_async",
    ]

    # Clean up any leftover tables from prior runs
    for table_name in async_table_names:
        drop_expr = DropTableExpression(
            dialect=dialect,
            table_name=table_name,
            if_exists=True,
        )
        sql, params = drop_expr.to_sql()
        await backend.execute(sql, params)

    # --- test_vector_items_async ---
    create_items = CreateTableExpression(
        dialect=dialect,
        table_name="test_vector_items_async",
        columns=[_id_column(), _embedding_column()],
    )
    sql, params = create_items.to_sql()
    await backend.execute(sql, params)

    insert_items = InsertExpression(
        dialect=dialect,
        into="test_vector_items_async",
        columns=["embedding"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "[1.0, 2.0, 3.0]").cast("vector")],
                [Literal(dialect, "[4.0, 5.0, 6.0]").cast("vector")],
            ],
        ),
    )
    sql, params = insert_items.to_sql()
    await backend.execute(sql, params)

    # --- test_vector_l2_async ---
    create_l2 = CreateTableExpression(
        dialect=dialect,
        table_name="test_vector_l2_async",
        columns=[_id_column(), _embedding_column()],
    )
    sql, params = create_l2.to_sql()
    await backend.execute(sql, params)

    insert_l2 = InsertExpression(
        dialect=dialect,
        into="test_vector_l2_async",
        columns=["embedding"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "[1.0, 0.0, 0.0]").cast("vector")],
                [Literal(dialect, "[0.0, 1.0, 0.0]").cast("vector")],
                [Literal(dialect, "[0.0, 0.0, 1.0]").cast("vector")],
            ],
        ),
    )
    sql, params = insert_l2.to_sql()
    await backend.execute(sql, params)

    # --- test_vector_cosine_async ---
    create_cosine = CreateTableExpression(
        dialect=dialect,
        table_name="test_vector_cosine_async",
        columns=[_id_column(), _embedding_column()],
    )
    sql, params = create_cosine.to_sql()
    await backend.execute(sql, params)

    insert_cosine = InsertExpression(
        dialect=dialect,
        into="test_vector_cosine_async",
        columns=["embedding"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "[1.0, 0.0, 0.0]").cast("vector")],
                [Literal(dialect, "[1.0, 1.0, 0.0]").cast("vector")],
                [Literal(dialect, "[0.0, 0.0, 1.0]").cast("vector")],
            ],
        ),
    )
    sql, params = insert_cosine.to_sql()
    await backend.execute(sql, params)

    # --- test_vector_ip_async ---
    create_ip = CreateTableExpression(
        dialect=dialect,
        table_name="test_vector_ip_async",
        columns=[_id_column(), _embedding_column()],
    )
    sql, params = create_ip.to_sql()
    await backend.execute(sql, params)

    insert_ip = InsertExpression(
        dialect=dialect,
        into="test_vector_ip_async",
        columns=["embedding"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "[1.0, 2.0, 3.0]").cast("vector")],
                [Literal(dialect, "[-1.0, 0.0, 0.0]").cast("vector")],
            ],
        ),
    )
    sql, params = insert_ip.to_sql()
    await backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop tables
    for table_name in [
        "test_vector_items_async",
        "test_vector_l2_async",
        "test_vector_cosine_async",
        "test_vector_ip_async",
    ]:
        drop_expr = DropTableExpression(
            dialect=dialect,
            table_name=table_name,
            if_exists=True,
        )
        sql, params = drop_expr.to_sql()
        await backend.execute(sql, params)


class TestAsyncPgvectorIntegration:
    """Async integration tests for pgvector vector similarity search."""

    @pytest.mark.asyncio
    async def test_async_vector_type_crud(self, async_vector_env):
        """Test querying vector column data."""
        backend, dialect = async_vector_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "embedding")],
            from_=TableExpression(dialect, "test_vector_items_async"),
            order_by=OrderByClause(dialect, [Column(dialect, "id")]),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 2
        assert result.data[0]['embedding'] is not None
        assert result.data[1]['embedding'] is not None

    @pytest.mark.asyncio
    async def test_async_l2_distance(self, async_vector_env):
        """Test L2 (Euclidean) distance operator <->."""
        backend, dialect = async_vector_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        distance_expr = RawSQLExpression(
            dialect, "embedding <-> '[1.0, 0.0, 0.0]' AS distance"
        )

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), distance_expr],
            from_=TableExpression(dialect, "test_vector_l2_async"),
            order_by=OrderByClause(
                dialect,
                # TODO: Replace with ComparisonPredicate when it supports .as_() alias
                [RawSQLExpression(dialect, "embedding <-> '[1.0, 0.0, 0.0]'")],
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 3
        assert float(result.data[0]['distance']) == pytest.approx(0.0, abs=1e-6)
        assert float(result.data[1]['distance']) == pytest.approx(1.41421356, abs=1e-4)

    @pytest.mark.asyncio
    async def test_async_cosine_distance(self, async_vector_env):
        """Test cosine distance operator <=>."""
        backend, dialect = async_vector_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        distance_expr = RawSQLExpression(
            dialect, "embedding <=> '[1.0, 0.0, 0.0]' AS distance"
        )

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), distance_expr],
            from_=TableExpression(dialect, "test_vector_cosine_async"),
            order_by=OrderByClause(
                dialect,
                # TODO: Replace with ComparisonPredicate when it supports .as_() alias
                [RawSQLExpression(dialect, "embedding <=> '[1.0, 0.0, 0.0]'")],
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 3
        assert float(result.data[0]['distance']) == pytest.approx(0.0, abs=1e-6)
        assert float(result.data[1]['distance']) == pytest.approx(0.29289, abs=1e-3)

    @pytest.mark.asyncio
    async def test_async_inner_product(self, async_vector_env):
        """Test inner product operator <#>."""
        backend, dialect = async_vector_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        ip_expr = RawSQLExpression(
            dialect, "embedding <#> '[1.0, 0.0, 0.0]' AS ip"
        )

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), ip_expr],
            from_=TableExpression(dialect, "test_vector_ip_async"),
            order_by=OrderByClause(
                dialect,
                # TODO: Replace with ComparisonPredicate when it supports .as_() alias
                [RawSQLExpression(dialect, "embedding <#> '[1.0, 0.0, 0.0]'")],
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 2
        assert float(result.data[0]['ip']) == pytest.approx(-1.0, abs=1e-4)

    @pytest.mark.asyncio
    async def test_async_vector_update(self, async_vector_env):
        """Test updating vector values."""
        backend, dialect = async_vector_env

        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="test_vector_update_async",
            columns=[_id_column(), _embedding_column()],
        )
        sql, params = create_expr.to_sql()
        await backend.execute(sql, params)

        try:
            insert_expr = InsertExpression(
                dialect=dialect,
                into="test_vector_update_async",
                columns=["embedding"],
                source=ValuesSource(
                    dialect,
                    [[Literal(dialect, "[1.0, 2.0, 3.0]").cast("vector")]],
                ),
            )
            sql, params = insert_expr.to_sql()
            await backend.execute(sql, params)

            update_expr = UpdateExpression(
                dialect=dialect,
                table="test_vector_update_async",
                assignments={"embedding": Literal(dialect, "[4.0, 5.0, 6.0]").cast("vector")},
                where=Column(dialect, "id") == Literal(dialect, 1),
            )
            sql, params = update_expr.to_sql()
            await backend.execute(sql, params)

            opts = ExecutionOptions(stmt_type=StatementType.DQL)
            query = QueryExpression(
                dialect=dialect,
                select=[Column(dialect, "embedding")],
                from_=TableExpression(dialect, "test_vector_update_async"),
                where=Column(dialect, "id") == Literal(dialect, 1),
            )
            sql, params = query.to_sql()
            result = await backend.execute(sql, params, options=opts)
            assert result.data is not None
            assert result.data[0]['embedding'] is not None
        finally:
            drop_expr = DropTableExpression(
                dialect=dialect,
                table_name="test_vector_update_async",
                if_exists=True,
            )
            sql, params = drop_expr.to_sql()
            await backend.execute(sql, params)

    @pytest.mark.asyncio
    async def test_async_ivfflat_index(self, async_vector_env):
        """Test creating an IVFFlat index on a vector column."""
        backend, dialect = async_vector_env

        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="test_vector_ivfflat_async",
            columns=[_id_column(), _embedding_column()],
        )
        sql, params = create_expr.to_sql()
        await backend.execute(sql, params)

        try:
            rows = [
                [Literal(dialect, f"[{i}.0, {i+1}.0, {i+2}.0]").cast("vector")]
                for i in range(10)
            ]
            insert_expr = InsertExpression(
                dialect=dialect,
                into="test_vector_ivfflat_async",
                columns=["embedding"],
                source=ValuesSource(dialect, rows),
            )
            sql, params = insert_expr.to_sql()
            await backend.execute(sql, params)

            # CREATE INDEX with opclass and WITH parameters is not supported
            # by the expression system, so raw SQL is used here.
            await backend.execute(
                "CREATE INDEX idx_test_ivfflat_async ON test_vector_ivfflat_async "
                "USING ivfflat (embedding vector_cosine_ops) WITH (lists = 5)"
            )

            opts = ExecutionOptions(stmt_type=StatementType.DQL)
            query = QueryExpression(
                dialect=dialect,
                select=[Column(dialect, "indexname")],
                from_=TableExpression(dialect, "pg_indexes"),
                where=Column(dialect, "tablename") == Literal(dialect, "test_vector_ivfflat_async"),
            )
            sql, params = query.to_sql()
            result = await backend.execute(sql, params, options=opts)
            assert result.data is not None
            index_names = [r['indexname'] for r in result.data]
            assert "idx_test_ivfflat_async" in index_names
        finally:
            drop_expr = DropTableExpression(
                dialect=dialect,
                table_name="test_vector_ivfflat_async",
                if_exists=True,
            )
            sql, params = drop_expr.to_sql()
            await backend.execute(sql, params)

    @pytest.mark.asyncio
    async def test_async_hnsw_index(self, async_vector_env):
        """Test creating an HNSW index on a vector column."""
        backend, dialect = async_vector_env

        if not dialect.check_extension_feature("vector", "hnsw_index"):
            pytest.skip("Extension 'vector' feature 'hnsw_index' not supported")

        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="test_vector_hnsw_async",
            columns=[_id_column(), _embedding_column()],
        )
        sql, params = create_expr.to_sql()
        await backend.execute(sql, params)

        try:
            rows = [
                [Literal(dialect, f"[{i}.0, {i+1}.0, {i+2}.0]").cast("vector")]
                for i in range(10)
            ]
            insert_expr = InsertExpression(
                dialect=dialect,
                into="test_vector_hnsw_async",
                columns=["embedding"],
                source=ValuesSource(dialect, rows),
            )
            sql, params = insert_expr.to_sql()
            await backend.execute(sql, params)

            # CREATE INDEX with opclass is not supported by the expression
            # system, so raw SQL is used here.
            await backend.execute(
                "CREATE INDEX idx_test_hnsw_async ON test_vector_hnsw_async "
                "USING hnsw (embedding vector_cosine_ops)"
            )

            opts = ExecutionOptions(stmt_type=StatementType.DQL)
            query = QueryExpression(
                dialect=dialect,
                select=[Column(dialect, "indexname")],
                from_=TableExpression(dialect, "pg_indexes"),
                where=Column(dialect, "tablename") == Literal(dialect, "test_vector_hnsw_async"),
            )
            sql, params = query.to_sql()
            result = await backend.execute(sql, params, options=opts)
            assert result.data is not None
            index_names = [r['indexname'] for r in result.data]
            assert "idx_test_hnsw_async" in index_names
        finally:
            drop_expr = DropTableExpression(
                dialect=dialect,
                table_name="test_vector_hnsw_async",
                if_exists=True,
            )
            sql, params = drop_expr.to_sql()
            await backend.execute(sql, params)

    @pytest.mark.asyncio
    async def test_async_vector_dimension_constraint(self, async_vector_env):
        """Test that vector dimension constraint is enforced."""
        backend, dialect = async_vector_env

        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="test_vector_dim_async",
            columns=[_id_column(), _embedding_column()],
        )
        sql, params = create_expr.to_sql()
        await backend.execute(sql, params)

        try:
            insert_expr = InsertExpression(
                dialect=dialect,
                into="test_vector_dim_async",
                columns=["embedding"],
                source=ValuesSource(
                    dialect,
                    [[Literal(dialect, "[1.0, 2.0]").cast("vector")]],
                ),
            )
            sql, params = insert_expr.to_sql()
            with pytest.raises(DatabaseError):
                await backend.execute(sql, params)
        finally:
            drop_expr = DropTableExpression(
                dialect=dialect,
                table_name="test_vector_dim_async",
                if_exists=True,
            )
            sql, params = drop_expr.to_sql()
            await backend.execute(sql, params)
