# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_tablefunc_integration.py
"""
Integration tests for PostgreSQL tablefunc extension with real database.

These tests require a live PostgreSQL connection with tablefunc extension installed
and test:
- normal_rand() function for generating normally distributed random values
- crosstab() function for producing pivot table displays
- connectby() function for tree-structured display of hierarchical data

Note on set-returning functions (SRFs):
- normal_rand() works in the SELECT clause directly.
- crosstab() and connectby() return SETOF record and require the FROM clause
  syntax with column type definitions (e.g., ``SELECT * FROM crosstab('...')
  AS ct(month TEXT, electronics INT, clothing INT)``). The current expression
  system's TableFunctionExpression does not support column type definitions,
  so these SRFs are tested using raw SQL execution via backend.execute().
  The function factories are still validated for correct expression generation.
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
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.impl.postgres.functions.tablefunc import (
    normal_rand,
    crosstab,
    connectby,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


@pytest.fixture
def tablefunc_env(postgres_backend_single):
    """Independent test environment for tablefunc extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "tablefunc")
    dialect = backend.dialect

    # Setup: create test_tf_sales table using expression
    sales_columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="month", data_type="TEXT"),
        ColumnDefinition(name="category", data_type="TEXT"),
        ColumnDefinition(name="amount", data_type="INT"),
    ]
    create_sales = CreateTableExpression(
        dialect=dialect,
        table_name="test_tf_sales",
        columns=sales_columns,
        if_not_exists=True,
    )
    sql, params = create_sales.to_sql()
    backend.execute(sql, params)

    # Setup: insert seed data for sales
    sales_rows = [
        [Literal(dialect, "Jan"), Literal(dialect, "Electronics"), Literal(dialect, 100)],
        [Literal(dialect, "Jan"), Literal(dialect, "Clothing"), Literal(dialect, 200)],
        [Literal(dialect, "Feb"), Literal(dialect, "Electronics"), Literal(dialect, 150)],
        [Literal(dialect, "Feb"), Literal(dialect, "Clothing"), Literal(dialect, 250)],
        [Literal(dialect, "Mar"), Literal(dialect, "Electronics"), Literal(dialect, 120)],
        [Literal(dialect, "Mar"), Literal(dialect, "Clothing"), Literal(dialect, 180)],
    ]
    insert_sales = InsertExpression(
        dialect=dialect,
        into="test_tf_sales",
        columns=["month", "category", "amount"],
        source=ValuesSource(dialect, sales_rows),
    )
    sql, params = insert_sales.to_sql()
    backend.execute(sql, params)

    # Setup: create test_tf_tree table using expression
    tree_columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="node_name", data_type="TEXT"),
        ColumnDefinition(name="parent_id", data_type="INT"),
    ]
    create_tree = CreateTableExpression(
        dialect=dialect,
        table_name="test_tf_tree",
        columns=tree_columns,
        if_not_exists=True,
    )
    sql, params = create_tree.to_sql()
    backend.execute(sql, params)

    # Setup: insert hierarchical data (root -> child1, child2 -> grandchild)
    tree_rows = [
        [Literal(dialect, "root"), Literal(dialect, None)],
        [Literal(dialect, "child1"), Literal(dialect, 1)],
        [Literal(dialect, "child2"), Literal(dialect, 1)],
        [Literal(dialect, "grandchild"), Literal(dialect, 2)],
    ]
    insert_tree = InsertExpression(
        dialect=dialect,
        into="test_tf_tree",
        columns=["node_name", "parent_id"],
        source=ValuesSource(dialect, tree_rows),
    )
    sql, params = insert_tree.to_sql()
    backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop both tables using DropTableExpression
    drop_sales = DropTableExpression(
        dialect=dialect,
        table_name="test_tf_sales",
        if_exists=True,
    )
    sql, params = drop_sales.to_sql()
    backend.execute(sql, params)

    drop_tree = DropTableExpression(
        dialect=dialect,
        table_name="test_tf_tree",
        if_exists=True,
    )
    sql, params = drop_tree.to_sql()
    backend.execute(sql, params)


class TestTablefuncIntegration:
    """Integration tests for tablefunc extension functions."""

    def test_normal_rand(self, tablefunc_env):
        """Test normal_rand() generates normally distributed random values."""
        backend, dialect = tablefunc_env

        # Generate 5 random values with mean=100.0 and stddev=15.0
        rand_func = normal_rand(dialect, 5, 100.0, 15.0)
        query = QueryExpression(
            dialect=dialect,
            select=[rand_func.as_("random_value")],
        )
        sql, params = query.to_sql()
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 5
        # Verify values are numeric (normally distributed around mean=100)
        for row in result.data:
            assert row["random_value"] is not None

    def test_crosstab_expression_generation(self, tablefunc_env):
        """Test crosstab() factory produces correct FunctionCall expression."""
        backend, dialect = tablefunc_env

        # Verify single-argument form expression generation
        crosstab_func = crosstab(
            dialect,
            "SELECT month, category, amount FROM test_tf_sales ORDER BY 1",
        )
        sql, params = crosstab_func.to_sql()
        assert "CROSSTAB" in sql
        assert len(params) == 1
        assert "test_tf_sales" in str(params[0])

        # Verify two-argument form expression generation
        crosstab_func2 = crosstab(
            dialect,
            "SELECT month, category, amount FROM test_tf_sales ORDER BY 1",
            "SELECT DISTINCT category FROM test_tf_sales ORDER BY 1",
        )
        sql2, params2 = crosstab_func2.to_sql()
        assert "CROSSTAB" in sql2
        assert len(params2) == 2

    def test_crosstab_execution(self, tablefunc_env):
        """Test crosstab() produces a pivot table via raw SQL.

        crosstab() is a set-returning function that returns SETOF record.
        It requires the FROM clause syntax with column type definitions,
        which is not supported by the current TableFunctionExpression
        (it lacks column type support). Therefore, we execute the full
        crosstab query using raw SQL.
        """
        backend, dialect = tablefunc_env

        # Execute crosstab query with column type definitions
        sql = (
            "SELECT * FROM crosstab("
            "'SELECT month, category, amount FROM test_tf_sales ORDER BY 1'"
            ") AS ct(month TEXT, electronics INT, clothing INT)"
        )
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = backend.execute(sql, options=opts)
        assert result.data is not None
        assert len(result.data) > 0
        # Verify pivot structure: each row should have month and two category columns
        for row in result.data:
            assert "month" in row
            assert "electronics" in row
            assert "clothing" in row

    def test_crosstab_with_categories_execution(self, tablefunc_env):
        """Test crosstab() with categories_sql for consistent column ordering via raw SQL.

        The two-argument form of crosstab uses an explicit category query
        to ensure consistent column ordering in the pivot output.
        """
        backend, dialect = tablefunc_env

        sql = (
            "SELECT * FROM crosstab("
            "'SELECT month, category, amount FROM test_tf_sales ORDER BY 1',"
            "'SELECT DISTINCT category FROM test_tf_sales ORDER BY 1'"
            ") AS ct(month TEXT, electronics INT, clothing INT)"
        )
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = backend.execute(sql, options=opts)
        assert result.data is not None
        assert len(result.data) > 0
        for row in result.data:
            assert "month" in row

    def test_connectby_expression_generation(self, tablefunc_env):
        """Test connectby() factory produces correct FunctionCall expression."""
        backend, dialect = tablefunc_env

        # Verify basic form expression generation
        connectby_func = connectby(
            dialect,
            "test_tf_tree",
            "id",
            "parent_id",
            "1",
            max_depth=3,
        )
        sql, params = connectby_func.to_sql()
        assert "CONNECTBY" in sql
        # 5 args: table_name, key_column, parent_column, start_value, max_depth
        assert len(params) == 5

        # Verify with branch_delim expression generation
        connectby_func2 = connectby(
            dialect,
            "test_tf_tree",
            "id",
            "parent_id",
            "1",
            max_depth=3,
            branch_delim="~",
        )
        sql2, params2 = connectby_func2.to_sql()
        assert "CONNECTBY" in sql2
        # 6 args: table_name, key_column, parent_column, start_value, max_depth, branch_delim
        assert len(params2) == 6

    def test_connectby_execution(self, tablefunc_env):
        """Test connectby() traverses hierarchical tree data via raw SQL.

        connectby() is a set-returning function that returns SETOF record.
        It requires the FROM clause syntax with column type definitions,
        which is not supported by the current TableFunctionExpression.
        Therefore, we execute the full connectby query using raw SQL.
        """
        backend, dialect = tablefunc_env

        # connectby traverses tree starting from root (id=1), max depth=3
        sql = (
            "SELECT * FROM connectby("
            "'test_tf_tree', 'id', 'parent_id', '1', 3"
            ") AS tree(id INT, parent_id INT, level INT)"
        )
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = backend.execute(sql, options=opts)
        assert result.data is not None
        # Should return rows for root, child1, child2, and grandchild
        assert len(result.data) > 0
        for row in result.data:
            assert "id" in row
            assert "level" in row

    def test_connectby_with_branch_delim_execution(self, tablefunc_env):
        """Test connectby() with branch delimiter for path display via raw SQL.

        When branch_delim is provided, connectby produces an additional
        branch column showing the path from root to each node.
        """
        backend, dialect = tablefunc_env

        sql = (
            "SELECT * FROM connectby("
            "'test_tf_tree', 'id', 'parent_id', '1', 3, '~'"
            ") AS tree(id INT, parent_id INT, level INT, branch TEXT)"
        )
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = backend.execute(sql, options=opts)
        assert result.data is not None
        assert len(result.data) > 0
        for row in result.data:
            assert "id" in row
            assert "branch" in row


@pytest_asyncio.fixture
async def async_tablefunc_env(async_postgres_backend_single):
    """Independent async test environment for tablefunc extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "tablefunc")
    dialect = backend.dialect

    # Setup: create test_tf_sales_async table using expression
    sales_columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="month", data_type="TEXT"),
        ColumnDefinition(name="category", data_type="TEXT"),
        ColumnDefinition(name="amount", data_type="INT"),
    ]
    create_sales = CreateTableExpression(
        dialect=dialect,
        table_name="test_tf_sales_async",
        columns=sales_columns,
        if_not_exists=True,
    )
    sql, params = create_sales.to_sql()
    await backend.execute(sql, params)

    # Setup: insert seed data for sales
    sales_rows = [
        [Literal(dialect, "Jan"), Literal(dialect, "Electronics"), Literal(dialect, 100)],
        [Literal(dialect, "Jan"), Literal(dialect, "Clothing"), Literal(dialect, 200)],
        [Literal(dialect, "Feb"), Literal(dialect, "Electronics"), Literal(dialect, 150)],
        [Literal(dialect, "Feb"), Literal(dialect, "Clothing"), Literal(dialect, 250)],
        [Literal(dialect, "Mar"), Literal(dialect, "Electronics"), Literal(dialect, 120)],
        [Literal(dialect, "Mar"), Literal(dialect, "Clothing"), Literal(dialect, 180)],
    ]
    insert_sales = InsertExpression(
        dialect=dialect,
        into="test_tf_sales_async",
        columns=["month", "category", "amount"],
        source=ValuesSource(dialect, sales_rows),
    )
    sql, params = insert_sales.to_sql()
    await backend.execute(sql, params)

    # Setup: create test_tf_tree_async table using expression
    tree_columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="node_name", data_type="TEXT"),
        ColumnDefinition(name="parent_id", data_type="INT"),
    ]
    create_tree = CreateTableExpression(
        dialect=dialect,
        table_name="test_tf_tree_async",
        columns=tree_columns,
        if_not_exists=True,
    )
    sql, params = create_tree.to_sql()
    await backend.execute(sql, params)

    # Setup: insert hierarchical data (root -> child1, child2 -> grandchild)
    tree_rows = [
        [Literal(dialect, "root"), Literal(dialect, None)],
        [Literal(dialect, "child1"), Literal(dialect, 1)],
        [Literal(dialect, "child2"), Literal(dialect, 1)],
        [Literal(dialect, "grandchild"), Literal(dialect, 2)],
    ]
    insert_tree = InsertExpression(
        dialect=dialect,
        into="test_tf_tree_async",
        columns=["node_name", "parent_id"],
        source=ValuesSource(dialect, tree_rows),
    )
    sql, params = insert_tree.to_sql()
    await backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop both tables using DropTableExpression
    drop_sales = DropTableExpression(
        dialect=dialect,
        table_name="test_tf_sales_async",
        if_exists=True,
    )
    sql, params = drop_sales.to_sql()
    await backend.execute(sql, params)

    drop_tree = DropTableExpression(
        dialect=dialect,
        table_name="test_tf_tree_async",
        if_exists=True,
    )
    sql, params = drop_tree.to_sql()
    await backend.execute(sql, params)


class TestAsyncTablefuncIntegration:
    """Async integration tests for tablefunc extension functions."""

    @pytest.mark.asyncio
    async def test_async_normal_rand(self, async_tablefunc_env):
        """Test normal_rand() generates normally distributed random values asynchronously."""
        backend, dialect = async_tablefunc_env

        # Generate 5 random values with mean=100.0 and stddev=15.0
        rand_func = normal_rand(dialect, 5, 100.0, 15.0)
        query = QueryExpression(
            dialect=dialect,
            select=[rand_func.as_("random_value")],
        )
        sql, params = query.to_sql()
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 5
        # Verify values are numeric (normally distributed around mean=100)
        for row in result.data:
            assert row["random_value"] is not None

    @pytest.mark.asyncio
    async def test_async_crosstab_expression_generation(self, async_tablefunc_env):
        """Test crosstab() factory produces correct FunctionCall expression asynchronously."""
        backend, dialect = async_tablefunc_env

        # Verify single-argument form expression generation
        crosstab_func = crosstab(
            dialect,
            "SELECT month, category, amount FROM test_tf_sales_async ORDER BY 1",
        )
        sql, params = crosstab_func.to_sql()
        assert "CROSSTAB" in sql
        assert len(params) == 1
        assert "test_tf_sales_async" in str(params[0])

        # Verify two-argument form expression generation
        crosstab_func2 = crosstab(
            dialect,
            "SELECT month, category, amount FROM test_tf_sales_async ORDER BY 1",
            "SELECT DISTINCT category FROM test_tf_sales_async ORDER BY 1",
        )
        sql2, params2 = crosstab_func2.to_sql()
        assert "CROSSTAB" in sql2
        assert len(params2) == 2

    @pytest.mark.asyncio
    async def test_async_crosstab_execution(self, async_tablefunc_env):
        """Test crosstab() produces a pivot table via raw SQL asynchronously.

        crosstab() is a set-returning function that returns SETOF record.
        It requires the FROM clause syntax with column type definitions,
        which is not supported by the current TableFunctionExpression
        (it lacks column type support). Therefore, we execute the full
        crosstab query using raw SQL.
        """
        backend, dialect = async_tablefunc_env

        # Execute crosstab query with column type definitions
        sql = (
            "SELECT * FROM crosstab("
            "'SELECT month, category, amount FROM test_tf_sales_async ORDER BY 1'"
            ") AS ct(month TEXT, electronics INT, clothing INT)"
        )
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = await backend.execute(sql, options=opts)
        assert result.data is not None
        assert len(result.data) > 0
        # Verify pivot structure: each row should have month and two category columns
        for row in result.data:
            assert "month" in row
            assert "electronics" in row
            assert "clothing" in row

    @pytest.mark.asyncio
    async def test_async_crosstab_with_categories_execution(self, async_tablefunc_env):
        """Test crosstab() with categories_sql for consistent column ordering via raw SQL asynchronously.

        The two-argument form of crosstab uses an explicit category query
        to ensure consistent column ordering in the pivot output.
        """
        backend, dialect = async_tablefunc_env

        sql = (
            "SELECT * FROM crosstab("
            "'SELECT month, category, amount FROM test_tf_sales_async ORDER BY 1',"
            "'SELECT DISTINCT category FROM test_tf_sales_async ORDER BY 1'"
            ") AS ct(month TEXT, electronics INT, clothing INT)"
        )
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = await backend.execute(sql, options=opts)
        assert result.data is not None
        assert len(result.data) > 0
        for row in result.data:
            assert "month" in row

    @pytest.mark.asyncio
    async def test_async_connectby_expression_generation(self, async_tablefunc_env):
        """Test connectby() factory produces correct FunctionCall expression asynchronously."""
        backend, dialect = async_tablefunc_env

        # Verify basic form expression generation
        connectby_func = connectby(
            dialect,
            "test_tf_tree_async",
            "id",
            "parent_id",
            "1",
            max_depth=3,
        )
        sql, params = connectby_func.to_sql()
        assert "CONNECTBY" in sql
        # 5 args: table_name, key_column, parent_column, start_value, max_depth
        assert len(params) == 5

        # Verify with branch_delim expression generation
        connectby_func2 = connectby(
            dialect,
            "test_tf_tree_async",
            "id",
            "parent_id",
            "1",
            max_depth=3,
            branch_delim="~",
        )
        sql2, params2 = connectby_func2.to_sql()
        assert "CONNECTBY" in sql2
        # 6 args: table_name, key_column, parent_column, start_value, max_depth, branch_delim
        assert len(params2) == 6

    @pytest.mark.asyncio
    async def test_async_connectby_execution(self, async_tablefunc_env):
        """Test connectby() traverses hierarchical tree data via raw SQL asynchronously.

        connectby() is a set-returning function that returns SETOF record.
        It requires the FROM clause syntax with column type definitions,
        which is not supported by the current TableFunctionExpression.
        Therefore, we execute the full connectby query using raw SQL.
        """
        backend, dialect = async_tablefunc_env

        # connectby traverses tree starting from root (id=1), max depth=3
        sql = (
            "SELECT * FROM connectby("
            "'test_tf_tree_async', 'id', 'parent_id', '1', 3"
            ") AS tree(id INT, parent_id INT, level INT)"
        )
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = await backend.execute(sql, options=opts)
        assert result.data is not None
        # Should return rows for root, child1, child2, and grandchild
        assert len(result.data) > 0
        for row in result.data:
            assert "id" in row
            assert "level" in row

    @pytest.mark.asyncio
    async def test_async_connectby_with_branch_delim_execution(self, async_tablefunc_env):
        """Test connectby() with branch delimiter for path display via raw SQL asynchronously.

        When branch_delim is provided, connectby produces an additional
        branch column showing the path from root to each node.
        """
        backend, dialect = async_tablefunc_env

        sql = (
            "SELECT * FROM connectby("
            "'test_tf_tree_async', 'id', 'parent_id', '1', 3, '~'"
            ") AS tree(id INT, parent_id INT, level INT, branch TEXT)"
        )
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = await backend.execute(sql, options=opts)
        assert result.data is not None
        assert len(result.data) > 0
        for row in result.data:
            assert "id" in row
            assert "branch" in row
