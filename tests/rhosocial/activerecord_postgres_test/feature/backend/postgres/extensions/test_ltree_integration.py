# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_ltree_integration.py

"""Integration tests for PostgreSQL ltree extension with real database.

These tests require a live PostgreSQL connection with ltree extension installed
and test:
- ltree path operations (@>, <@, ~)
- Ancestor/descendant queries
- nlevel and subpath functions
- GiST index on ltree columns
- ltxtquery search (@ operator)
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
    CreateIndexExpression,
    OrderByClause,
    RawSQLExpression,
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal, FunctionCall
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


# --- Sync fixture and tests ---


@pytest.fixture
def ltree_env(postgres_backend_single):
    """Independent test environment for ltree extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "ltree")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in ["test_ltree", "test_ltree_tree", "test_ltree_func",
                       "test_ltree_idx", "test_ltree_txtq"]:
        drop_expr = DropTableExpression(dialect=dialect, table_name=table_name, if_exists=True)
        sql, params = drop_expr.to_sql()
        backend.execute(sql, params)

    # Setup: create test_ltree table
    create_ltree = CreateTableExpression(
        dialect=dialect,
        table_name="test_ltree",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="SERIAL",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="path", data_type="LTREE"),
        ],
        if_not_exists=True,
    )
    sql, params = create_ltree.to_sql()
    backend.execute(sql, params)

    # Insert data into test_ltree
    insert_ltree = InsertExpression(
        dialect=dialect,
        into="test_ltree",
        columns=["path"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "Top").cast("ltree")],
                [Literal(dialect, "Top.Science").cast("ltree")],
                [Literal(dialect, "Top.Science.Astronomy").cast("ltree")],
                [Literal(dialect, "Top.Science.Physics").cast("ltree")],
                [Literal(dialect, "Top.History").cast("ltree")],
            ],
        ),
    )
    sql, params = insert_ltree.to_sql()
    backend.execute(sql, params)

    # Setup: create test_ltree_tree table
    create_tree = CreateTableExpression(
        dialect=dialect,
        table_name="test_ltree_tree",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="SERIAL",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="path", data_type="LTREE"),
            ColumnDefinition(name="label", data_type="TEXT"),
        ],
        if_not_exists=True,
    )
    sql, params = create_tree.to_sql()
    backend.execute(sql, params)

    # Insert data into test_ltree_tree
    insert_tree = InsertExpression(
        dialect=dialect,
        into="test_ltree_tree",
        columns=["path", "label"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "root").cast("ltree"), Literal(dialect, "Root")],
                [Literal(dialect, "root.level1").cast("ltree"), Literal(dialect, "Level 1")],
                [Literal(dialect, "root.level1.level2a").cast("ltree"), Literal(dialect, "Level 2a")],
                [Literal(dialect, "root.level1.level2b").cast("ltree"), Literal(dialect, "Level 2b")],
                [Literal(dialect, "root.other").cast("ltree"), Literal(dialect, "Other branch")],
            ],
        ),
    )
    sql, params = insert_tree.to_sql()
    backend.execute(sql, params)

    # Setup: create test_ltree_func table
    create_func = CreateTableExpression(
        dialect=dialect,
        table_name="test_ltree_func",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="SERIAL",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="path", data_type="LTREE"),
        ],
        if_not_exists=True,
    )
    sql, params = create_func.to_sql()
    backend.execute(sql, params)

    # Insert data into test_ltree_func
    insert_func = InsertExpression(
        dialect=dialect,
        into="test_ltree_func",
        columns=["path"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, "A.B.C.D").cast("ltree")]],
        ),
    )
    sql, params = insert_func.to_sql()
    backend.execute(sql, params)

    # Setup: create test_ltree_idx table
    create_idx_table = CreateTableExpression(
        dialect=dialect,
        table_name="test_ltree_idx",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="SERIAL",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="path", data_type="LTREE"),
        ],
        if_not_exists=True,
    )
    sql, params = create_idx_table.to_sql()
    backend.execute(sql, params)

    # Insert data into test_ltree_idx
    insert_idx = InsertExpression(
        dialect=dialect,
        into="test_ltree_idx",
        columns=["path"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "a.b.c").cast("ltree")],
                [Literal(dialect, "a.b.d").cast("ltree")],
                [Literal(dialect, "x.y.z").cast("ltree")],
            ],
        ),
    )
    sql, params = insert_idx.to_sql()
    backend.execute(sql, params)

    # Create GiST index on test_ltree_idx
    create_index = CreateIndexExpression(
        dialect=dialect,
        index_name="idx_ltree_path",
        table_name="test_ltree_idx",
        columns=["path"],
        index_type="GIST",
        if_not_exists=True,
    )
    sql, params = create_index.to_sql()
    backend.execute(sql, params)

    # Setup: create test_ltree_txtq table
    create_txtq = CreateTableExpression(
        dialect=dialect,
        table_name="test_ltree_txtq",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="SERIAL",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="path", data_type="LTREE"),
        ],
        if_not_exists=True,
    )
    sql, params = create_txtq.to_sql()
    backend.execute(sql, params)

    # Insert data into test_ltree_txtq
    insert_txtq = InsertExpression(
        dialect=dialect,
        into="test_ltree_txtq",
        columns=["path"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "Top.Science.Astronomy").cast("ltree")],
                [Literal(dialect, "Top.Science.Physics").cast("ltree")],
                [Literal(dialect, "Top.History").cast("ltree")],
            ],
        ),
    )
    sql, params = insert_txtq.to_sql()
    backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop tables
    for table_name in [
        "test_ltree",
        "test_ltree_tree",
        "test_ltree_func",
        "test_ltree_idx",
        "test_ltree_txtq",
    ]:
        drop_expr = DropTableExpression(
            dialect=dialect,
            table_name=table_name,
            if_exists=True,
        )
        sql, params = drop_expr.to_sql()
        backend.execute(sql, params)


class TestLtreeIntegration:
    """Integration tests for ltree extension."""

    def test_ltree_path_operations(self, ltree_env):
        """Test ltree path operators @>, <@, ~ with table data."""
        backend, dialect = ltree_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test @> (ancestor) operator: does 'Top.Science' contain 'Top.Science.Astronomy'?
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        ancestor_expr = RawSQLExpression(
            dialect, "'Top.Science'::ltree @> path AS is_ancestor"
        )
        query = QueryExpression(
            dialect=dialect,
            select=[ancestor_expr, Column(dialect, "path")],
            from_=TableExpression(dialect, "test_ltree"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "path"),
                Literal(dialect, "Top.Science.Astronomy").cast("ltree"),
            ),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['is_ancestor'] is True

        # 'Top.History' does not contain 'Top.Science.Astronomy'
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        not_ancestor_expr = RawSQLExpression(
            dialect, "'Top.History'::ltree @> path AS is_ancestor"
        )
        query2 = QueryExpression(
            dialect=dialect,
            select=[not_ancestor_expr, Column(dialect, "path")],
            from_=TableExpression(dialect, "test_ltree"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "path"),
                Literal(dialect, "Top.Science.Astronomy").cast("ltree"),
            ),
        )
        sql2, params2 = query2.to_sql()
        result2 = backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        assert result2.data[0]['is_ancestor'] is False

        # Test <@ (descendant) operator
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        descendant_expr = RawSQLExpression(
            dialect, "path <@ 'Top.Science'::ltree AS is_descendant"
        )
        query3 = QueryExpression(
            dialect=dialect,
            select=[descendant_expr, Column(dialect, "path")],
            from_=TableExpression(dialect, "test_ltree"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "path"),
                Literal(dialect, "Top.Science.Astronomy").cast("ltree"),
            ),
        )
        sql3, params3 = query3.to_sql()
        result3 = backend.execute(sql3, params3, options=opts)
        assert result3.data is not None
        assert result3.data[0]['is_descendant'] is True

        # Test ~ (lquery match) operator: find paths matching pattern
        # Use Literal in SELECT to compute the match, then filter in Python
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        match_expr = RawSQLExpression(
            dialect, "path ~ 'Top.Science.*'::lquery AS matches"
        )
        query4 = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "path"), match_expr],
            from_=TableExpression(dialect, "test_ltree"),
            order_by=OrderByClause(dialect, [Column(dialect, "path")]),
        )
        sql4, params4 = query4.to_sql()
        result4 = backend.execute(sql4, params4, options=opts)
        assert result4.data is not None
        matched = [r for r in result4.data if r['matches'] is True]
        paths = [r['path'] for r in matched]
        assert len(matched) >= 2
        assert 'Top.Science' in paths
        assert 'Top.Science.Astronomy' in paths

    def test_ltree_ancestor_descendant(self, ltree_env):
        """Test finding all ancestors and descendants of a path."""
        backend, dialect = ltree_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Find all ancestors of 'root.level1.level2a'
        # Use @> operator via Literal in SELECT, then filter in Python
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        ancestor_expr = RawSQLExpression(
            dialect, "path @> 'root.level1.level2a'::ltree AS is_ancestor"
        )
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "path"), Column(dialect, "label"), ancestor_expr],
            from_=TableExpression(dialect, "test_ltree_tree"),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        ancestors = [r for r in result.data if r['is_ancestor'] is True]
        assert len(ancestors) == 3  # root, root.level1, root.level1.level2a
        labels = [r['label'] for r in ancestors]
        assert 'Root' in labels
        assert 'Level 1' in labels
        assert 'Level 2a' in labels

        # Find all descendants of 'root.level1'
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        descendant_expr = RawSQLExpression(
            dialect, "path <@ 'root.level1'::ltree AS is_descendant"
        )
        query2 = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "path"), Column(dialect, "label"), descendant_expr],
            from_=TableExpression(dialect, "test_ltree_tree"),
            order_by=OrderByClause(dialect, [Column(dialect, "path")]),
        )
        sql2, params2 = query2.to_sql()
        result2 = backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        descendants = [r for r in result2.data if r['is_descendant'] is True]
        assert len(descendants) == 3  # root.level1, root.level1.level2a, root.level1.level2b
        labels2 = [r['label'] for r in descendants]
        assert 'Level 1' in labels2
        assert 'Level 2a' in labels2
        assert 'Level 2b' in labels2

    def test_ltree_nlevel_subpath(self, ltree_env):
        """Test nlevel and subpath functions."""
        backend, dialect = ltree_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test nlevel: count number of labels
        nlevel_func = FunctionCall(
            dialect, "nlevel", Column(dialect, "path")
        ).as_("depth")
        query = QueryExpression(
            dialect=dialect,
            select=[nlevel_func],
            from_=TableExpression(dialect, "test_ltree_func"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['depth'] == 4

        # Test subpath: get first 2 levels
        subpath_func = FunctionCall(
            dialect, "subpath",
            Column(dialect, "path"),
            Literal(dialect, 0),
            Literal(dialect, 2),
        ).as_("sub")
        query2 = QueryExpression(
            dialect=dialect,
            select=[subpath_func],
            from_=TableExpression(dialect, "test_ltree_func"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql2, params2 = query2.to_sql()
        result2 = backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        assert result2.data[0]['sub'] is not None
        assert 'A.B' in str(result2.data[0]['sub'])

        # Test subpath: get remaining from offset 2
        subpath_func2 = FunctionCall(
            dialect, "subpath",
            Column(dialect, "path"),
            Literal(dialect, 2),
        ).as_("sub")
        query3 = QueryExpression(
            dialect=dialect,
            select=[subpath_func2],
            from_=TableExpression(dialect, "test_ltree_func"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql3, params3 = query3.to_sql()
        result3 = backend.execute(sql3, params3, options=opts)
        assert result3.data is not None
        assert result3.data[0]['sub'] is not None
        assert 'C.D' in str(result3.data[0]['sub'])

    def test_ltree_gist_index(self, ltree_env):
        """Test creating GiST index on ltree column."""
        backend, dialect = ltree_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Verify index exists
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        query = QueryExpression(
            dialect=dialect,
            select=[RawSQLExpression(dialect, "COUNT(*) AS cnt")],
            from_=TableExpression(dialect, "pg_indexes"),
            where=ComparisonPredicate(
                dialect, "AND",
                ComparisonPredicate(
                    dialect, "=",
                    Column(dialect, "tablename"),
                    Literal(dialect, "test_ltree_idx"),
                ),
                ComparisonPredicate(
                    dialect, "=",
                    Column(dialect, "indexname"),
                    Literal(dialect, "idx_ltree_path"),
                ),
            ),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['cnt'] >= 1

        # Insert data and query using index - use Literal for <@ operator in SELECT
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        descendant_expr = RawSQLExpression(
            dialect, "path <@ 'a.b'::ltree AS is_descendant"
        )
        query2 = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "path"), descendant_expr],
            from_=TableExpression(dialect, "test_ltree_idx"),
            order_by=OrderByClause(dialect, [Column(dialect, "path")]),
        )
        sql2, params2 = query2.to_sql()
        result2 = backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        matched = [r for r in result2.data if r['is_descendant'] is True]
        assert len(matched) >= 2

    def test_ltree_ltxtquery_search(self, ltree_env):
        """Test ltxtquery for full-text-like search on ltree paths."""
        backend, dialect = ltree_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # @ operator with ltxtquery: paths matching 'Science & Astronomy'
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        match_expr = RawSQLExpression(
            dialect, "path @ 'Science & Astronomy'::ltxtquery AS matches"
        )
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "path"), match_expr],
            from_=TableExpression(dialect, "test_ltree_txtq"),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        matched = [r for r in result.data if r['matches'] is True]
        assert len(matched) >= 1
        paths = [r['path'] for r in matched]
        assert 'Top.Science.Astronomy' in paths


# --- Async fixture and tests ---


@pytest_asyncio.fixture
async def async_ltree_env(async_postgres_backend_single):
    """Independent async test environment for ltree extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "ltree")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in ["test_ltree_async", "test_ltree_tree_async",
                       "test_ltree_func_async", "test_ltree_idx_async",
                       "test_ltree_txtq_async"]:
        drop_expr = DropTableExpression(dialect=dialect, table_name=table_name, if_exists=True)
        sql, params = drop_expr.to_sql()
        await backend.execute(sql, params)

    # Setup: create test_ltree_async table
    create_ltree = CreateTableExpression(
        dialect=dialect,
        table_name="test_ltree_async",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="SERIAL",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="path", data_type="LTREE"),
        ],
        if_not_exists=True,
    )
    sql, params = create_ltree.to_sql()
    await backend.execute(sql, params)

    # Insert data into test_ltree_async
    insert_ltree = InsertExpression(
        dialect=dialect,
        into="test_ltree_async",
        columns=["path"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "Top").cast("ltree")],
                [Literal(dialect, "Top.Science").cast("ltree")],
                [Literal(dialect, "Top.Science.Astronomy").cast("ltree")],
                [Literal(dialect, "Top.Science.Physics").cast("ltree")],
                [Literal(dialect, "Top.History").cast("ltree")],
            ],
        ),
    )
    sql, params = insert_ltree.to_sql()
    await backend.execute(sql, params)

    # Setup: create test_ltree_tree_async table
    create_tree = CreateTableExpression(
        dialect=dialect,
        table_name="test_ltree_tree_async",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="SERIAL",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="path", data_type="LTREE"),
            ColumnDefinition(name="label", data_type="TEXT"),
        ],
        if_not_exists=True,
    )
    sql, params = create_tree.to_sql()
    await backend.execute(sql, params)

    # Insert data into test_ltree_tree_async
    insert_tree = InsertExpression(
        dialect=dialect,
        into="test_ltree_tree_async",
        columns=["path", "label"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "root").cast("ltree"), Literal(dialect, "Root")],
                [Literal(dialect, "root.level1").cast("ltree"), Literal(dialect, "Level 1")],
                [Literal(dialect, "root.level1.level2a").cast("ltree"), Literal(dialect, "Level 2a")],
                [Literal(dialect, "root.level1.level2b").cast("ltree"), Literal(dialect, "Level 2b")],
                [Literal(dialect, "root.other").cast("ltree"), Literal(dialect, "Other branch")],
            ],
        ),
    )
    sql, params = insert_tree.to_sql()
    await backend.execute(sql, params)

    # Setup: create test_ltree_func_async table
    create_func = CreateTableExpression(
        dialect=dialect,
        table_name="test_ltree_func_async",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="SERIAL",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="path", data_type="LTREE"),
        ],
        if_not_exists=True,
    )
    sql, params = create_func.to_sql()
    await backend.execute(sql, params)

    # Insert data into test_ltree_func_async
    insert_func = InsertExpression(
        dialect=dialect,
        into="test_ltree_func_async",
        columns=["path"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, "A.B.C.D").cast("ltree")]],
        ),
    )
    sql, params = insert_func.to_sql()
    await backend.execute(sql, params)

    # Setup: create test_ltree_idx_async table
    create_idx_table = CreateTableExpression(
        dialect=dialect,
        table_name="test_ltree_idx_async",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="SERIAL",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="path", data_type="LTREE"),
        ],
        if_not_exists=True,
    )
    sql, params = create_idx_table.to_sql()
    await backend.execute(sql, params)

    # Insert data into test_ltree_idx_async
    insert_idx = InsertExpression(
        dialect=dialect,
        into="test_ltree_idx_async",
        columns=["path"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "a.b.c").cast("ltree")],
                [Literal(dialect, "a.b.d").cast("ltree")],
                [Literal(dialect, "x.y.z").cast("ltree")],
            ],
        ),
    )
    sql, params = insert_idx.to_sql()
    await backend.execute(sql, params)

    # Create GiST index on test_ltree_idx_async
    create_index = CreateIndexExpression(
        dialect=dialect,
        index_name="idx_ltree_path_async",
        table_name="test_ltree_idx_async",
        columns=["path"],
        index_type="GIST",
        if_not_exists=True,
    )
    sql, params = create_index.to_sql()
    await backend.execute(sql, params)

    # Setup: create test_ltree_txtq_async table
    create_txtq = CreateTableExpression(
        dialect=dialect,
        table_name="test_ltree_txtq_async",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="SERIAL",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="path", data_type="LTREE"),
        ],
        if_not_exists=True,
    )
    sql, params = create_txtq.to_sql()
    await backend.execute(sql, params)

    # Insert data into test_ltree_txtq_async
    insert_txtq = InsertExpression(
        dialect=dialect,
        into="test_ltree_txtq_async",
        columns=["path"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "Top.Science.Astronomy").cast("ltree")],
                [Literal(dialect, "Top.Science.Physics").cast("ltree")],
                [Literal(dialect, "Top.History").cast("ltree")],
            ],
        ),
    )
    sql, params = insert_txtq.to_sql()
    await backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop tables
    for table_name in [
        "test_ltree_async",
        "test_ltree_tree_async",
        "test_ltree_func_async",
        "test_ltree_idx_async",
        "test_ltree_txtq_async",
    ]:
        drop_expr = DropTableExpression(
            dialect=dialect,
            table_name=table_name,
            if_exists=True,
        )
        sql, params = drop_expr.to_sql()
        await backend.execute(sql, params)


class TestAsyncLtreeIntegration:
    """Async integration tests for ltree extension."""

    @pytest.mark.asyncio
    async def test_async_ltree_path_operations(self, async_ltree_env):
        """Test ltree path operators @>, <@, ~ with table data."""
        backend, dialect = async_ltree_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test @> (ancestor) operator: does 'Top.Science' contain 'Top.Science.Astronomy'?
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        ancestor_expr = RawSQLExpression(
            dialect, "'Top.Science'::ltree @> path AS is_ancestor"
        )
        query = QueryExpression(
            dialect=dialect,
            select=[ancestor_expr, Column(dialect, "path")],
            from_=TableExpression(dialect, "test_ltree_async"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "path"),
                Literal(dialect, "Top.Science.Astronomy").cast("ltree"),
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['is_ancestor'] is True

        # 'Top.History' does not contain 'Top.Science.Astronomy'
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        not_ancestor_expr = RawSQLExpression(
            dialect, "'Top.History'::ltree @> path AS is_ancestor"
        )
        query2 = QueryExpression(
            dialect=dialect,
            select=[not_ancestor_expr, Column(dialect, "path")],
            from_=TableExpression(dialect, "test_ltree_async"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "path"),
                Literal(dialect, "Top.Science.Astronomy").cast("ltree"),
            ),
        )
        sql2, params2 = query2.to_sql()
        result2 = await backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        assert result2.data[0]['is_ancestor'] is False

        # Test <@ (descendant) operator
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        descendant_expr = RawSQLExpression(
            dialect, "path <@ 'Top.Science'::ltree AS is_descendant"
        )
        query3 = QueryExpression(
            dialect=dialect,
            select=[descendant_expr, Column(dialect, "path")],
            from_=TableExpression(dialect, "test_ltree_async"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "path"),
                Literal(dialect, "Top.Science.Astronomy").cast("ltree"),
            ),
        )
        sql3, params3 = query3.to_sql()
        result3 = await backend.execute(sql3, params3, options=opts)
        assert result3.data is not None
        assert result3.data[0]['is_descendant'] is True

        # Test ~ (lquery match) operator: find paths matching pattern
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        match_expr = RawSQLExpression(
            dialect, "path ~ 'Top.Science.*'::lquery AS matches"
        )
        query4 = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "path"), match_expr],
            from_=TableExpression(dialect, "test_ltree_async"),
            order_by=OrderByClause(dialect, [Column(dialect, "path")]),
        )
        sql4, params4 = query4.to_sql()
        result4 = await backend.execute(sql4, params4, options=opts)
        assert result4.data is not None
        matched = [r for r in result4.data if r['matches'] is True]
        paths = [r['path'] for r in matched]
        assert len(matched) >= 2
        assert 'Top.Science' in paths
        assert 'Top.Science.Astronomy' in paths

    @pytest.mark.asyncio
    async def test_async_ltree_ancestor_descendant(self, async_ltree_env):
        """Test finding all ancestors and descendants of a path."""
        backend, dialect = async_ltree_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Find all ancestors of 'root.level1.level2a'
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        ancestor_expr = RawSQLExpression(
            dialect, "path @> 'root.level1.level2a'::ltree AS is_ancestor"
        )
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "path"), Column(dialect, "label"), ancestor_expr],
            from_=TableExpression(dialect, "test_ltree_tree_async"),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        ancestors = [r for r in result.data if r['is_ancestor'] is True]
        assert len(ancestors) == 3  # root, root.level1, root.level1.level2a
        labels = [r['label'] for r in ancestors]
        assert 'Root' in labels
        assert 'Level 1' in labels
        assert 'Level 2a' in labels

        # Find all descendants of 'root.level1'
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        descendant_expr = RawSQLExpression(
            dialect, "path <@ 'root.level1'::ltree AS is_descendant"
        )
        query2 = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "path"), Column(dialect, "label"), descendant_expr],
            from_=TableExpression(dialect, "test_ltree_tree_async"),
            order_by=OrderByClause(dialect, [Column(dialect, "path")]),
        )
        sql2, params2 = query2.to_sql()
        result2 = await backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        descendants = [r for r in result2.data if r['is_descendant'] is True]
        assert len(descendants) == 3  # root.level1, root.level1.level2a, root.level1.level2b
        labels2 = [r['label'] for r in descendants]
        assert 'Level 1' in labels2
        assert 'Level 2a' in labels2
        assert 'Level 2b' in labels2

    @pytest.mark.asyncio
    async def test_async_ltree_nlevel_subpath(self, async_ltree_env):
        """Test nlevel and subpath functions."""
        backend, dialect = async_ltree_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test nlevel: count number of labels
        nlevel_func = FunctionCall(
            dialect, "nlevel", Column(dialect, "path")
        ).as_("depth")
        query = QueryExpression(
            dialect=dialect,
            select=[nlevel_func],
            from_=TableExpression(dialect, "test_ltree_func_async"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['depth'] == 4

        # Test subpath: get first 2 levels
        subpath_func = FunctionCall(
            dialect, "subpath",
            Column(dialect, "path"),
            Literal(dialect, 0),
            Literal(dialect, 2),
        ).as_("sub")
        query2 = QueryExpression(
            dialect=dialect,
            select=[subpath_func],
            from_=TableExpression(dialect, "test_ltree_func_async"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql2, params2 = query2.to_sql()
        result2 = await backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        assert result2.data[0]['sub'] is not None
        assert 'A.B' in str(result2.data[0]['sub'])

        # Test subpath: get remaining from offset 2
        subpath_func2 = FunctionCall(
            dialect, "subpath",
            Column(dialect, "path"),
            Literal(dialect, 2),
        ).as_("sub")
        query3 = QueryExpression(
            dialect=dialect,
            select=[subpath_func2],
            from_=TableExpression(dialect, "test_ltree_func_async"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql3, params3 = query3.to_sql()
        result3 = await backend.execute(sql3, params3, options=opts)
        assert result3.data is not None
        assert result3.data[0]['sub'] is not None
        assert 'C.D' in str(result3.data[0]['sub'])

    @pytest.mark.asyncio
    async def test_async_ltree_gist_index(self, async_ltree_env):
        """Test creating GiST index on ltree column."""
        backend, dialect = async_ltree_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Verify index exists
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        query = QueryExpression(
            dialect=dialect,
            select=[RawSQLExpression(dialect, "COUNT(*) AS cnt")],
            from_=TableExpression(dialect, "pg_indexes"),
            where=ComparisonPredicate(
                dialect, "AND",
                ComparisonPredicate(
                    dialect, "=",
                    Column(dialect, "tablename"),
                    Literal(dialect, "test_ltree_idx_async"),
                ),
                ComparisonPredicate(
                    dialect, "=",
                    Column(dialect, "indexname"),
                    Literal(dialect, "idx_ltree_path_async"),
                ),
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['cnt'] >= 1

        # Insert data and query using index - use Literal for <@ operator in SELECT
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        descendant_expr = RawSQLExpression(
            dialect, "path <@ 'a.b'::ltree AS is_descendant"
        )
        query2 = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "path"), descendant_expr],
            from_=TableExpression(dialect, "test_ltree_idx_async"),
            order_by=OrderByClause(dialect, [Column(dialect, "path")]),
        )
        sql2, params2 = query2.to_sql()
        result2 = await backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        matched = [r for r in result2.data if r['is_descendant'] is True]
        assert len(matched) >= 2

    @pytest.mark.asyncio
    async def test_async_ltree_ltxtquery_search(self, async_ltree_env):
        """Test ltxtquery for full-text-like search on ltree paths."""
        backend, dialect = async_ltree_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # @ operator with ltxtquery: paths matching 'Science & Astronomy'
        # TODO: Replace with ComparisonPredicate when it supports .as_() alias
        match_expr = RawSQLExpression(
            dialect, "path @ 'Science & Astronomy'::ltxtquery AS matches"
        )
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "path"), match_expr],
            from_=TableExpression(dialect, "test_ltree_txtq_async"),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        matched = [r for r in result.data if r['matches'] is True]
        assert len(matched) >= 1
        paths = [r['path'] for r in matched]
        assert 'Top.Science.Astronomy' in paths
