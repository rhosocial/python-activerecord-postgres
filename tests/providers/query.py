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
from typing import Type, List, Tuple
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.testsuite.feature.query.interfaces import IQueryProvider

# Import model classes inside each method to avoid circular imports
# The scenarios are defined specifically for this backend.
from .scenarios import get_enabled_scenarios, get_scenario


class QueryProvider(IQueryProvider):
    """
    This is the postgres backend's implementation for the query features test group.
    It connects the generic tests in the testsuite with the actual postgres database.
    """

    def __init__(self):
        # Track backend instances used in the current test session
        self._active_backends = []

    def get_test_scenarios(self) -> List[str]:
        """Returns a list of names for all enabled scenarios for this backend."""
        return list(get_enabled_scenarios().keys())

    def _setup_model(self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str, shared_backend=None) -> Type[ActiveRecord]:
        """A generic helper method to handle the setup for any given model."""
        # 1. Get the backend class (PostgresBackend) and connection config for the requested scenario.
        backend_class, config = get_scenario(scenario_name)

        # 2. Configure the generic model class with our specific backend and config.
        #    If a shared backend is provided, reuse it for all models in the group
        if shared_backend is None:
            # Create a new backend instance
            model_class.configure(config, backend_class)
        else:
            # Reuse the shared backend instance for subsequent models in the group
            model_class.__connection_config__ = config
            model_class.__backend_class__ = backend_class
            model_class.__backend__ = shared_backend

        # Track the backend instance if it's not already tracked
        backend_instance = model_class.__backend__
        if backend_instance not in self._active_backends:
            self._active_backends.append(backend_instance)

        # 3. Prepare the database schema. To ensure tests are isolated, we drop
        #    the table if it exists and recreate it from the schema file.
        try:
            # Drop the table if it exists, using CASCADE to handle foreign key dependencies
            backend_instance.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
        except Exception:
            # Continue anyway since the table might not exist
            pass

        schema_sql = self._load_postgres_schema(f"{table_name}.sql")
        backend_instance.execute(schema_sql)

        return model_class

    def _setup_multiple_models(self, models_and_tables: List[Tuple[Type[ActiveRecord], str]], scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """A helper to set up multiple models for fixture groups, sharing the same backend."""
        result = []
        shared_backend = None
        for i, (model_class, table_name) in enumerate(models_and_tables):
            if i == 0:
                # For the first model, create a new backend instance
                configured_model = self._setup_model(model_class, scenario_name, table_name)
                shared_backend = configured_model.__backend__
            else:
                # For subsequent models, reuse the shared backend instance
                configured_model = self._setup_model(model_class, scenario_name, table_name, shared_backend=shared_backend)
            result.append(configured_model)
        return tuple(result)

    # --- Implementation of the IQueryProvider interface ---

    def setup_order_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for order-related models (User, Order, OrderItem) tests."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models import User, Order, OrderItem
        models_and_tables = [
            (User, "users"),
            (Order, "orders"),
            (OrderItem, "order_items")
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    def setup_blog_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for blog-related models (User, Post, Comment) tests."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models import User, Post, Comment
        models_and_tables = [
            (User, "users"),
            (Post, "posts"),
            (Comment, "comments")
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    def setup_json_user_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """Sets up the database for the JSON user model."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models import JsonUser
        models_and_tables = [
            (JsonUser, "json_users"),
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    def setup_tree_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """Sets up the database for the tree structure (Node) model."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.cte_models import Node
        models_and_tables = [
            (Node, "nodes"),
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    def setup_extended_order_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for extended order-related models (User, ExtendedOrder, ExtendedOrderItem) tests."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.extended_models import User, ExtendedOrder, ExtendedOrderItem
        models_and_tables = [
            (User, "users"),
            (ExtendedOrder, "extended_orders"),
            (ExtendedOrderItem, "extended_order_items")
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    def setup_combined_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for combined models (User, Order, OrderItem, Post, Comment) tests."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models import User, Order, OrderItem, Post, Comment
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
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import MappedUser, MappedPost, MappedComment
        return self._setup_multiple_models([
            (MappedUser, "users"),
            (MappedPost, "posts"),
            (MappedComment, "comments")
        ], scenario_name)

    # --- Async implementations ---

    async def setup_async_order_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for async order-related models (AsyncUser, AsyncOrder, AsyncOrderItem) tests."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models import AsyncUser, AsyncOrder, AsyncOrderItem
        models_and_tables = [
            (AsyncUser, "users"),
            (AsyncOrder, "orders"),
            (AsyncOrderItem, "order_items")
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    async def setup_async_blog_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for async blog-related models (AsyncUser, AsyncPost, AsyncComment) tests."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models import AsyncUser, AsyncPost, AsyncComment
        models_and_tables = [
            (AsyncUser, "users"),
            (AsyncPost, "posts"),
            (AsyncComment, "comments")
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    async def setup_async_json_user_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """Sets up the database for the async JSON user model."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models import AsyncJsonUser
        models_and_tables = [
            (AsyncJsonUser, "json_users"),
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    async def setup_async_tree_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """Sets up the database for the async tree structure (AsyncNode) model."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.cte_models import AsyncNode
        models_and_tables = [
            (AsyncNode, "nodes"),
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    async def setup_async_extended_order_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for async extended order-related models (AsyncUser, AsyncExtendedOrder, AsyncExtendedOrderItem) tests."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.extended_models import AsyncUser, AsyncExtendedOrder, AsyncExtendedOrderItem
        models_and_tables = [
            (AsyncUser, "users"),
            (AsyncExtendedOrder, "extended_orders"),
            (AsyncExtendedOrderItem, "extended_order_items")
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    async def setup_async_combined_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for async combined models (AsyncUser, AsyncOrder, AsyncOrderItem, AsyncPost, AsyncComment) tests."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models import AsyncUser, AsyncOrder, AsyncOrderItem, AsyncPost, AsyncComment
        models_and_tables = [
            (AsyncUser, "users"),
            (AsyncOrder, "orders"),
            (AsyncOrderItem, "order_items"),
            (AsyncPost, "posts"),
            (AsyncComment, "comments")
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    async def setup_async_annotated_query_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """Sets up the database for the async SearchableItem model tests."""
        from rhosocial.activerecord.testsuite.feature.query.fixtures.annotated_adapter_models import AsyncSearchableItem
        return self._setup_multiple_models([
            (AsyncSearchableItem, "searchable_items"),
        ], scenario_name)

    async def setup_async_mapped_models(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for AsyncMappedUser, AsyncMappedPost, and AsyncMappedComment models."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import AsyncMappedUser, AsyncMappedPost, AsyncMappedComment
        return self._setup_multiple_models([
            (AsyncMappedUser, "users"),
            (AsyncMappedPost, "posts"),
            (AsyncMappedComment, "comments")
        ], scenario_name)

    async def cleanup_after_test_async(self, scenario_name: str):
        """
        Performs async cleanup after a test, dropping all tables and disconnecting backends.
        """
        self.cleanup_after_test(scenario_name)

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
