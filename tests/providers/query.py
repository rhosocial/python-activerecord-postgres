"""
This file provides the concrete implementation of the `IQueryProvider` interface
that is defined in the `rhosocial-activerecord-testsuite` package.

Its main responsibilities are:
1.  Reporting which test scenarios (database configurations) are available.
2.  Setting up the database environment for a given test. This includes:
    - Getting the correct database configuration for the scenario.
    - Configuring the ActiveRecord model with a database connection.
    - Dropping any old tables and creating the necessary table schema.
3.  Cleaning up any resources (like temporary database files) after a test runs.
"""
import os
import sys
import logging
from typing import Type, List, Tuple

from rhosocial.activerecord.model import ActiveRecord

# Setup logging for fixture selection debugging
logger = logging.getLogger(__name__)

# Import the fixture selector utility
from rhosocial.activerecord.testsuite.utils import select_fixture

# Import base version models (Python 3.8+)
from rhosocial.activerecord.testsuite.feature.query.fixtures.models import (
    User as UserBase, JsonUser as JsonUserBase,
    Order as OrderBase, OrderItem as OrderItemBase,
    Post as PostBase, Comment as CommentBase,
    MappedUser as MappedUserBase, MappedPost as MappedPostBase, MappedComment as MappedCommentBase
)
from rhosocial.activerecord.testsuite.feature.query.fixtures.cte_models import Node
from rhosocial.activerecord.testsuite.feature.query.fixtures.extended_models import ExtendedOrder, ExtendedOrderItem

# Conditionally import Python 3.10+ models
User310 = JsonUser310 = Order310 = OrderItem310 = Post310 = Comment310 = None
MappedUser310 = MappedPost310 = MappedComment310 = None

if sys.version_info >= (3, 10):
    try:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models_py310 import (
            User as User310, JsonUser as JsonUser310,
            Order as Order310, OrderItem as OrderItem310,
            Post as Post310, Comment as Comment310,
            MappedUser as MappedUser310, MappedPost as MappedPost310, MappedComment as MappedComment310
        )
    except ImportError as e:
        logger.warning(f"Failed to import Python 3.10+ fixtures: {e}")

# Conditionally import Python 3.11+ models
User311 = JsonUser311 = Order311 = OrderItem311 = Post311 = Comment311 = None
MappedUser311 = MappedPost311 = MappedComment311 = None

if sys.version_info >= (3, 11):
    try:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models_py311 import (
            User as User311, JsonUser as JsonUser311,
            Order as Order311, OrderItem as OrderItem311,
            Post as Post311, Comment as Comment311,
            MappedUser as MappedUser311, MappedPost as MappedPost311, MappedComment as MappedComment311
        )
    except ImportError as e:
        logger.warning(f"Failed to import Python 3.11+ fixtures: {e}")

# Conditionally import Python 3.12+ models
User312 = JsonUser312 = Order312 = OrderItem312 = Post312 = Comment312 = None
MappedUser312 = MappedPost312 = MappedComment312 = None

if sys.version_info >= (3, 12):
    try:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models_py312 import (
            User as User312, JsonUser as JsonUser312,
            Order as Order312, OrderItem as OrderItem312,
            Post as Post312, Comment as Comment312,
            MappedUser as MappedUser312, MappedPost as MappedPost312, MappedComment as MappedComment312
        )
    except ImportError as e:
        logger.warning(f"Failed to import Python 3.12+ fixtures: {e}")


# Select appropriate fixture classes based on Python version
def _select_model_class(base_cls, py312_cls, py311_cls, py310_cls, model_name: str) -> Type:
    """Select the most appropriate model class for the current Python version."""
    candidates = [c for c in [py312_cls, py311_cls, py310_cls, base_cls] if c is not None]
    selected = select_fixture(*candidates)
    logger.info(f"Selected {model_name}: {selected.__name__} from {selected.__module__}")
    return selected


# Select sync models
User = _select_model_class(UserBase, User312, User311, User310, "User")
JsonUser = _select_model_class(JsonUserBase, JsonUser312, JsonUser311, JsonUser310, "JsonUser")
Order = _select_model_class(OrderBase, Order312, Order311, Order310, "Order")
OrderItem = _select_model_class(OrderItemBase, OrderItem312, OrderItem311, OrderItem310, "OrderItem")
Post = _select_model_class(PostBase, Post312, Post311, Post310, "Post")
Comment = _select_model_class(CommentBase, Comment312, Comment311, Comment310, "Comment")
MappedUser = _select_model_class(MappedUserBase, MappedUser312, MappedUser311, MappedUser310, "MappedUser")
MappedPost = _select_model_class(MappedPostBase, MappedPost312, MappedPost311, MappedPost310, "MappedPost")
MappedComment = _select_model_class(MappedCommentBase, MappedComment312, MappedComment311, MappedComment310, "MappedComment")

from rhosocial.activerecord.testsuite.feature.query.interfaces import IQueryProvider
from rhosocial.activerecord.testsuite.core.protocols import WorkerTestProtocol
# The scenarios are defined specifically for this backend.
from .scenarios import get_enabled_scenarios, get_scenario


class QueryProvider(IQueryProvider, WorkerTestProtocol):
    """
    This is the postgres backend's implementation for the query features test group.
    It connects the generic tests in the testsuite with the actual postgres database.
    """

    def __init__(self):
        self._active_backends = []
        self._active_async_backends = []

    def get_test_scenarios(self) -> List[str]:
        """Returns a list of names for all enabled scenarios for this backend."""
        return list(get_enabled_scenarios().keys())

    def _track_backend(self, backend_instance, collection):
        if backend_instance not in collection:
            collection.append(backend_instance)

    def _setup_model(self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str, shared_backend=None) -> Type[ActiveRecord]:
        """A generic helper method to handle the setup for any given model."""
        backend_class, config = get_scenario(scenario_name)

        if shared_backend is None:
            model_class.configure(config, backend_class)
        else:
            model_class.__connection_config__ = config
            model_class.__backend_class__ = backend_class
            model_class.__backend__ = shared_backend

        backend_instance = model_class.__backend__
        self._track_backend(backend_instance, self._active_backends)

        try:
            backend_instance.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
        except Exception:
            pass

        schema_sql = self._load_postgres_schema(f"{table_name}.sql")
        backend_instance.execute(schema_sql)

        return model_class

    async def _setup_model_async(self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str, shared_backend=None) -> Type[ActiveRecord]:
        """A generic helper method to handle the setup for any given async model."""
        from rhosocial.activerecord.backend.impl.postgres import AsyncPostgresBackend

        _, config = get_scenario(scenario_name)

        if shared_backend is None:
            await model_class.configure(config, AsyncPostgresBackend)
        else:
            model_class.__connection_config__ = config
            model_class.__backend_class__ = AsyncPostgresBackend
            model_class.__backend__ = shared_backend

        backend_instance = model_class.__backend__
        self._track_backend(backend_instance, self._active_async_backends)

        try:
            await backend_instance.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
        except Exception:
            pass

        schema_sql = self._load_postgres_schema(f"{table_name}.sql")
        await backend_instance.execute(schema_sql)

        return model_class

    def _setup_multiple_models(self, models_and_tables: List[Tuple[Type[ActiveRecord], str]], scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """A helper to set up multiple models for fixture groups, sharing the same backend."""
        result = []
        shared_backend = None
        for i, (model_class, table_name) in enumerate(models_and_tables):
            if i == 0:
                configured_model = self._setup_model(model_class, scenario_name, table_name)
                shared_backend = configured_model.__backend__
            else:
                configured_model = self._setup_model(model_class, scenario_name, table_name, shared_backend=shared_backend)
            result.append(configured_model)
        return tuple(result)

    async def _setup_multiple_models_async(self, models_and_tables: List[Tuple[Type[ActiveRecord], str]], scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """A helper to set up multiple async models for fixture groups, sharing the same backend."""
        result = []
        shared_backend = None
        for i, (model_class, table_name) in enumerate(models_and_tables):
            if i == 0:
                configured_model = await self._setup_model_async(model_class, scenario_name, table_name)
                shared_backend = configured_model.__backend__
            else:
                configured_model = await self._setup_model_async(model_class, scenario_name, table_name, shared_backend=shared_backend)
            result.append(configured_model)
        return tuple(result)

    # --- Implementation of the IQueryProvider interface ---

    def setup_order_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for order-related models (User, Order, OrderItem) tests."""
        models_and_tables = [
            (User, "users"),
            (Order, "orders"),
            (OrderItem, "order_items")
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    def setup_blog_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for blog-related models (User, Post, Comment) tests."""
        models_and_tables = [
            (User, "users"),
            (Post, "posts"),
            (Comment, "comments")
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    def setup_json_user_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """Sets up the database for the JSON user model."""
        models_and_tables = [
            (JsonUser, "json_users"),
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    def setup_tree_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """Sets up the database for the tree structure (Node) model."""
        models_and_tables = [
            (Node, "nodes"),
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    def setup_extended_order_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for extended order-related models (User, ExtendedOrder, ExtendedOrderItem) tests."""
        models_and_tables = [
            (User, "users"),
            (ExtendedOrder, "extended_orders"),
            (ExtendedOrderItem, "extended_order_items")
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    def setup_combined_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for combined models (User, Order, OrderItem, Post, Comment) tests."""
        models_and_tables = [
            (User, "users"),
            (Order, "orders"),
            (OrderItem, "order_items"),
            (Post, "posts"),
            (Comment, "comments")
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    def setup_annotated_query_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """Sets up the database for the SearchableItem model tests."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.annotated_adapter_models import SearchableItem
        return self._setup_multiple_models([
            (SearchableItem, "searchable_items"),
        ], scenario_name)

    def setup_mapped_models(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for MappedUser, MappedPost, and MappedComment models."""
        return self._setup_multiple_models([
            (MappedUser, "users"),
            (MappedPost, "posts"),
            (MappedComment, "comments")
        ], scenario_name)

    # --- Async implementations ---

    async def setup_async_order_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for async order-related models (AsyncUser, AsyncOrder, AsyncOrderItem) tests."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_models import AsyncUser, AsyncOrder, AsyncOrderItem
        return await self._setup_multiple_models_async([
            (AsyncUser, "users"),
            (AsyncOrder, "orders"),
            (AsyncOrderItem, "order_items")
        ], scenario_name)

    async def setup_async_blog_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for async blog-related models (AsyncUser, AsyncPost, AsyncComment) tests."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_blog_models import AsyncUser, AsyncPost, AsyncComment
        return await self._setup_multiple_models_async([
            (AsyncUser, "users"),
            (AsyncPost, "posts"),
            (AsyncComment, "comments")
        ], scenario_name)

    async def setup_async_json_user_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """Sets up the database for the async JSON user model."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_json_models import AsyncJsonUser
        return await self._setup_multiple_models_async([
            (AsyncJsonUser, "json_users"),
        ], scenario_name)

    async def setup_async_tree_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """Sets up the database for the async tree structure (AsyncNode) model."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_cte_models import AsyncNode
        return await self._setup_multiple_models_async([
            (AsyncNode, "nodes"),
        ], scenario_name)

    async def setup_async_extended_order_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for async extended order-related models (AsyncUser, AsyncExtendedOrder, AsyncExtendedOrderItem) tests."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_extended_models import AsyncUser, AsyncExtendedOrder, AsyncExtendedOrderItem
        return await self._setup_multiple_models_async([
            (AsyncUser, "users"),
            (AsyncExtendedOrder, "extended_orders"),
            (AsyncExtendedOrderItem, "extended_order_items")
        ], scenario_name)

    async def setup_async_combined_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for async combined models (AsyncUser, AsyncOrder, AsyncOrderItem, AsyncPost, AsyncComment) tests."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_models import AsyncUser, AsyncOrder, AsyncOrderItem
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_blog_models import AsyncPost, AsyncComment
        return await self._setup_multiple_models_async([
            (AsyncUser, "users"),
            (AsyncOrder, "orders"),
            (AsyncOrderItem, "order_items"),
            (AsyncPost, "posts"),
            (AsyncComment, "comments")
        ], scenario_name)

    async def setup_async_annotated_query_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """Sets up the database for the async SearchableItem model tests."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_annotated_adapter_models import AsyncSearchableItem
        return await self._setup_multiple_models_async([
            (AsyncSearchableItem, "searchable_items"),
        ], scenario_name)

    async def setup_async_mapped_models(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for AsyncMappedUser, AsyncMappedPost, and AsyncMappedComment models."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import AsyncMappedUser, AsyncMappedPost, AsyncMappedComment
        return await self._setup_multiple_models_async([
            (AsyncMappedUser, "users"),
            (AsyncMappedPost, "posts"),
            (AsyncMappedComment, "comments")
        ], scenario_name)

    async def cleanup_after_test_async(self, scenario_name: str):
        """Performs async cleanup after a test."""
        tables_to_drop = [
            'users', 'orders', 'order_items', 'posts', 'comments', 'json_users', 'nodes',
            'extended_orders', 'extended_order_items', 'searchable_items'
        ]
        for backend_instance in self._active_async_backends:
            try:
                for table_name in tables_to_drop:
                    try:
                        await backend_instance.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
                    except Exception:
                        pass
            finally:
                try:
                    await backend_instance.disconnect()
                except:
                    pass
        self._active_async_backends.clear()

    def _load_postgres_schema(self, filename: str) -> str:
        """Helper to load a SQL schema file from this project's fixtures."""
        # Schemas are stored in the centralized location for query feature.
        schema_dir = os.path.join(os.path.dirname(__file__), "..", "rhosocial", "activerecord_postgres_test", "feature", "query", "schema")
        schema_path = os.path.join(schema_dir, filename)

        with open(schema_path, 'r', encoding='utf-8') as f:
            return f.read()

    def cleanup_after_test(self, scenario_name: str):
        """
        Performs cleanup after a test. This uses the same backend instance
        that was used for setup to ensure proper cleanup with foreign key constraints.
        """
        tables_to_drop = [
            'users', 'orders', 'order_items', 'posts', 'comments', 'json_users', 'nodes',
            'extended_orders', 'extended_order_items', 'searchable_items'
        ]
        for backend_instance in self._active_backends:
            try:
                # Drop all tables that might have been created for query tests
                # Use CASCADE to handle foreign key dependencies
                for table_name in tables_to_drop:
                    try:
                        backend_instance.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
                    except Exception:
                        # Continue with other tables if one fails
                        pass
            finally:
                # Always disconnect the backend instance that was used in the test
                try:
                    backend_instance.disconnect()
                except:
                    # Ignore errors during disconnect
                    pass

        # Clear the list of active backends for the next test
        self._active_backends.clear()

    # --- Implementation of WorkerTestProtocol ---

    def get_worker_connection_params(self, scenario_name: str, fixture_type: str = 'order') -> dict:
        """
        Return serializable connection parameters for Worker processes.

        This method provides all information needed to recreate the database
        connection in a Worker process, including the schema SQL for table creation.

        Args:
            scenario_name: The test scenario name
            fixture_type: Type of fixture ('order', 'blog', 'user', 'combined',
                         or with 'async_' prefix for async backends)

        Returns:
            Dictionary with connection parameters and schema SQL
        """
        from .scenarios import SCENARIO_MAP

        # Determine if async backend is needed based on fixture_type
        is_async = fixture_type and fixture_type.startswith('async_')
        backend_class_name = 'AsyncPostgresBackend' if is_async else 'PostgresBackend'

        # Get base fixture type (remove 'async_' prefix if present)
        base_fixture_type = fixture_type.replace('async_', '') if fixture_type else 'order'

        # Build schema SQL based on fixture type
        schema_sql = self._get_schema_sql_for_fixture_type(base_fixture_type)

        # Get connection config from scenario
        if scenario_name not in SCENARIO_MAP:
            if SCENARIO_MAP:
                scenario_name = next(iter(SCENARIO_MAP))
            else:
                raise ValueError("No scenarios registered")

        config_dict = SCENARIO_MAP[scenario_name]

        return {
            'backend_module': 'rhosocial.activerecord.backend.impl.postgres',
            'backend_class_name': backend_class_name,
            'config_class_module': 'rhosocial.activerecord.backend.impl.postgres.config',
            'config_class_name': 'PostgresConnectionConfig',
            'config_kwargs': config_dict,
            'schema_sql': schema_sql,
        }

    def get_worker_schema_sql(self, scenario_name: str, table_name: str) -> str:
        """
        Return the SQL statement to create a specific table.

        Args:
            scenario_name: The test scenario name (unused for Postgres as schema is fixed)
            table_name: Name of the table to create

        Returns:
            CREATE TABLE SQL statement
        """
        return self._load_postgres_schema(f'{table_name}.sql')

    def _get_schema_sql_for_fixture_type(self, fixture_type: str) -> dict:
        """
        Get schema SQL for a specific fixture type.

        Args:
            fixture_type: Type of fixture ('order', 'blog', 'user', 'combined')

        Returns:
            Dictionary mapping table names to CREATE TABLE statements
        """
        schemas = {}

        if fixture_type == 'order':
            tables = ['users', 'orders', 'order_items']
        elif fixture_type == 'blog':
            tables = ['users', 'posts', 'comments']
        elif fixture_type == 'user':
            tables = ['users']
        elif fixture_type == 'combined':
            tables = ['users', 'orders', 'order_items', 'posts', 'comments']
        else:
            tables = ['users']

        for table in tables:
            schemas[table] = self._load_postgres_schema(f'{table}.sql')

        return schemas
