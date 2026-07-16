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

from rhosocial.activerecord.model import ActiveRecord, AsyncActiveRecord

# Setup logging for fixture selection debugging
logger = logging.getLogger(__name__)

# Import the fixture selector utility
from rhosocial.activerecord.testsuite.utils import select_fixture  # noqa: E402

# Import base version models (Python 3.8+)
from rhosocial.activerecord.testsuite.feature.query.fixtures.models import (  # noqa: E402
    User as UserBase, JsonUser as JsonUserBase,
    Order as OrderBase, OrderItem as OrderItemBase,
    Post as PostBase, Comment as CommentBase,
    Profile as ProfileBase,
    MappedUser as MappedUserBase, MappedPost as MappedPostBase, MappedComment as MappedCommentBase
)
from rhosocial.activerecord.testsuite.feature.query.fixtures.cte_models import Node  # noqa: E402
from rhosocial.activerecord.testsuite.feature.query.fixtures.extended_models import ExtendedOrder, ExtendedOrderItem  # noqa: E402

# Conditionally import Python 3.10+ models
User310 = JsonUser310 = Order310 = OrderItem310 = Post310 = Comment310 = Profile310 = None
MappedUser310 = MappedPost310 = MappedComment310 = None

if sys.version_info >= (3, 10):
    try:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models_py310 import (
            User as User310, JsonUser as JsonUser310,
            Order as Order310, OrderItem as OrderItem310,
            Post as Post310, Comment as Comment310,
            Profile as Profile310,
            MappedUser as MappedUser310, MappedPost as MappedPost310, MappedComment as MappedComment310
        )
    except ImportError as e:
        logger.warning(f"Failed to import Python 3.10+ fixtures: {e}")

# Conditionally import Python 3.11+ models
User311 = JsonUser311 = Order311 = OrderItem311 = Post311 = Comment311 = Profile311 = None
MappedUser311 = MappedPost311 = MappedComment311 = None

if sys.version_info >= (3, 11):
    try:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models_py311 import (
            User as User311, JsonUser as JsonUser311,
            Order as Order311, OrderItem as OrderItem311,
            Post as Post311, Comment as Comment311,
            Profile as Profile311,
            MappedUser as MappedUser311, MappedPost as MappedPost311, MappedComment as MappedComment311
        )
    except ImportError as e:
        logger.warning(f"Failed to import Python 3.11+ fixtures: {e}")

# Conditionally import Python 3.12+ models
User312 = JsonUser312 = Order312 = OrderItem312 = Post312 = Comment312 = Profile312 = None
MappedUser312 = MappedPost312 = MappedComment312 = None

if sys.version_info >= (3, 12):
    try:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.models_py312 import (
            User as User312, JsonUser as JsonUser312,
            Order as Order312, OrderItem as OrderItem312,
            Post as Post312, Comment as Comment312,
            Profile as Profile312,
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
MappedComment = _select_model_class(MappedCommentBase, MappedComment312, MappedComment311, MappedComment310, "MappedComment")  # noqa: E501
Profile = _select_model_class(ProfileBase, Profile312, Profile311, Profile310, "Profile")

from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType  # noqa: E402
from rhosocial.activerecord.testsuite.feature.query.interfaces import IQuerySyncProvider, IQueryAsyncProvider  # noqa: E402
from rhosocial.activerecord.testsuite.core.protocols import WorkerTestProtocol  # noqa: E402

# Composite-PK model classes used by setup_order_item_model.
from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import (  # noqa: E402
    OrderItem as CompositeOrderItemBase,
    AsyncOrderItem as AsyncCompositeOrderItemBase,
)

# Expression-based DDL fixtures
from providers.fixtures._common import drop_table
from providers.fixtures.query import TABLE_EXPRESSIONS as QUERY_TABLE_EXPRESSIONS

# The scenarios are defined specifically for this backend.
from .scenarios import get_enabled_scenarios, get_scenario  # noqa: E402


class QueryProviderBase:
    def __init__(self):
        self._scenario_db_files = {}

    def get_test_scenarios(self) -> List[str]:
        return list(get_enabled_scenarios().keys())

    def _track_backend(self, backend_instance, collection):
        if backend_instance not in collection:
            collection.append(backend_instance)

    def _load_postgres_schema(self, filename: str) -> str:
        schema_dir = os.path.join(
            os.path.dirname(__file__), "..", "rhosocial", "activerecord_postgres_test", "feature", "query", "schema"
        )
        schema_path = os.path.join(schema_dir, filename)
        with open(schema_path, 'r', encoding='utf-8') as f:
            return f.read()


class QuerySyncProvider(QueryProviderBase, IQuerySyncProvider, WorkerTestProtocol):

    def __init__(self):
        super().__init__()
        self._active_backends = []

    def _setup_model(self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str, shared_backend=None) -> Type[ActiveRecord]:
        backend_class, config = get_scenario(scenario_name)
        if shared_backend is None:
            model_class.configure(config, backend_class)
        else:
            model_class.__connection_config__ = config
            model_class.__backend_class__ = backend_class
            model_class.__backend__ = shared_backend
        backend_instance = model_class.__backend__
        self._track_backend(backend_instance, self._active_backends)
        opts = ExecutionOptions(stmt_type=StatementType.DDL)
        try:
            sql, params = drop_table(backend_instance.dialect, table_name).to_sql()
            backend_instance.execute(sql, params, options=opts)
        except Exception:
            pass
        if fn := QUERY_TABLE_EXPRESSIONS.get(table_name):
            create_expr = fn(backend_instance.dialect, table_name)
            sql, params = create_expr.to_sql()
            backend_instance.execute(sql, params, options=opts)
        return model_class

    def _setup_multiple_models(self, models_and_tables: List[Tuple[Type[ActiveRecord], str]], scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
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

    def setup_order_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        models_and_tables = [
            (User, "users"),
            (Order, "orders"),
            (OrderItem, "order_items")
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    def setup_blog_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        models_and_tables = [
            (User, "users"),
            (Post, "posts"),
            (Comment, "comments")
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    def setup_json_user_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        from rhosocial.activerecord.backend.impl.postgres.adapters import PostgresJSONBAdapter
        jsonb_adapter = PostgresJSONBAdapter()
        json_fields = ["settings", "tags", "profile", "roles", "scores", "subscription", "preferences"]
        for field_name in json_fields:
            if field_name not in JsonUser.__field_adapters__:
                JsonUser.__field_adapters__[field_name] = (jsonb_adapter, str)
        models_and_tables = [
            (JsonUser, "json_users"),
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    def setup_tree_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        models_and_tables = [
            (Node, "nodes"),
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    def setup_extended_order_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        models_and_tables = [
            (User, "users"),
            (ExtendedOrder, "extended_orders"),
            (ExtendedOrderItem, "extended_order_items")
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    def setup_combined_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        models_and_tables = [
            (User, "users"),
            (Order, "orders"),
            (OrderItem, "order_items"),
            (Post, "posts"),
            (Comment, "comments")
        ]
        return self._setup_multiple_models(models_and_tables, scenario_name)

    def setup_annotated_query_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.annotated_adapter_models import SearchableItem
        return self._setup_multiple_models([
            (SearchableItem, "searchable_items"),
        ], scenario_name)

    def setup_mapped_models(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        return self._setup_multiple_models([
            (MappedUser, "users"),
            (MappedPost, "posts"),
            (MappedComment, "comments")
        ], scenario_name)

    def setup_profile_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord]]:
        return self._setup_multiple_models([
            (User, "users"),
            (Profile, "profiles"),
        ], scenario_name)

    def setup_order_item_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Set up the composite-PK OrderItem model for the query feature tests."""
        from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType
        from providers.fixtures._common import drop_table
        from providers.fixtures.basic import TABLE_EXPRESSIONS as BASIC_EXPRS

        backend_class, config = get_scenario(scenario_name)
        CompositeOrderItemBase.configure(config, backend_class)
        backend_instance = CompositeOrderItemBase.__backend__
        self._track_backend(backend_instance, self._active_backends)
        opts = ExecutionOptions(stmt_type=StatementType.DDL)
        try:
            sql, params = drop_table(backend_instance.dialect, "order_items").to_sql()
            backend_instance.execute(sql, params, options=opts)
        except Exception:
            pass
        if fn := BASIC_EXPRS.get("order_items"):
            create_expr = fn(backend_instance.dialect, "order_items")
            sql, params = create_expr.to_sql()
            backend_instance.execute(sql, params, options=opts)
        return CompositeOrderItemBase

    def _get_schema_sql_for_fixture_type(self, fixture_type: str) -> dict:
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

    def get_worker_connection_params(self, scenario_name: str, fixture_type: str = 'order') -> dict:
        from .scenarios import SCENARIO_MAP
        is_async = fixture_type and fixture_type.startswith('async_')
        backend_class_name = 'AsyncPostgresBackend' if is_async else 'PostgresBackend'
        base_fixture_type = fixture_type.replace('async_', '') if fixture_type else 'order'
        schema_sql = self._get_schema_sql_for_fixture_type(base_fixture_type)
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
        return self._load_postgres_schema(f'{table_name}.sql')

    def cleanup_after_test(self, scenario_name: str):
        tables_to_drop = [
            'users', 'orders', 'order_items', 'posts', 'comments', 'json_users', 'nodes',
            'extended_orders', 'extended_order_items', 'searchable_items'
        ]
        for backend_instance in self._active_backends:
            try:
                for table_name in tables_to_drop:
                    try:
                        backend_instance.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
                    except Exception:
                        pass
            finally:
                try:
                    backend_instance.disconnect()
                except:  # noqa: E722
                    pass
        self._active_backends.clear()


class QueryAsyncProvider(QueryProviderBase, IQueryAsyncProvider):

    def __init__(self):
        super().__init__()
        self._active_async_backends = []

    async def _setup_async_model(self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str, shared_backend=None) -> Type[ActiveRecord]:
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
        opts = ExecutionOptions(stmt_type=StatementType.DDL)
        try:
            sql, params = drop_table(backend_instance.dialect, table_name).to_sql()
            await backend_instance.execute(sql, params, options=opts)
        except Exception:
            pass
        if fn := QUERY_TABLE_EXPRESSIONS.get(table_name):
            create_expr = fn(backend_instance.dialect, table_name)
            sql, params = create_expr.to_sql()
            await backend_instance.execute(sql, params, options=opts)
        return model_class

    async def _setup_multiple_models_async(self, models_and_tables: List[Tuple[Type[ActiveRecord], str]], scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        result = []
        shared_backend = None
        for i, (model_class, table_name) in enumerate(models_and_tables):
            if i == 0:
                configured_model = await self._setup_async_model(model_class, scenario_name, table_name)
                shared_backend = configured_model.__backend__
            else:
                configured_model = await self._setup_async_model(model_class, scenario_name, table_name, shared_backend=shared_backend)
            result.append(configured_model)
        return tuple(result)

    async def setup_order_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_models import AsyncUser, AsyncOrder, AsyncOrderItem
        return await self._setup_multiple_models_async([
            (AsyncUser, "users"),
            (AsyncOrder, "orders"),
            (AsyncOrderItem, "order_items")
        ], scenario_name)

    async def setup_blog_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_models import AsyncUser
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_blog_models import AsyncPost, AsyncComment
        return await self._setup_multiple_models_async([
            (AsyncUser, "users"),
            (AsyncPost, "posts"),
            (AsyncComment, "comments")
        ], scenario_name)

    async def setup_json_user_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_json_models import AsyncJsonUser
        from rhosocial.activerecord.backend.impl.postgres.adapters import PostgresJSONBAdapter
        jsonb_adapter = PostgresJSONBAdapter()
        json_fields = ["settings", "tags", "profile", "roles", "scores", "subscription", "preferences"]
        for field_name in json_fields:
            if field_name not in AsyncJsonUser.__field_adapters__:
                AsyncJsonUser.__field_adapters__[field_name] = (jsonb_adapter, str)
        return await self._setup_multiple_models_async([
            (AsyncJsonUser, "json_users"),
        ], scenario_name)

    async def setup_tree_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_cte_models import AsyncNode
        return await self._setup_multiple_models_async([
            (AsyncNode, "nodes"),
        ], scenario_name)

    async def setup_extended_order_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_extended_models import AsyncUser, AsyncExtendedOrder, AsyncExtendedOrderItem
        return await self._setup_multiple_models_async([
            (AsyncUser, "users"),
            (AsyncExtendedOrder, "extended_orders"),
            (AsyncExtendedOrderItem, "extended_order_items")
        ], scenario_name)

    async def setup_combined_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_models import AsyncUser, AsyncOrder, AsyncOrderItem
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_blog_models import AsyncPost, AsyncComment
        return await self._setup_multiple_models_async([
            (AsyncUser, "users"),
            (AsyncOrder, "orders"),
            (AsyncOrderItem, "order_items"),
            (AsyncPost, "posts"),
            (AsyncComment, "comments")
        ], scenario_name)

    async def setup_annotated_query_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_annotated_adapter_models import AsyncSearchableItem
        return await self._setup_multiple_models_async([
            (AsyncSearchableItem, "searchable_items"),
        ], scenario_name)

    async def setup_mapped_models(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import AsyncMappedUser, AsyncMappedPost, AsyncMappedComment
        return await self._setup_multiple_models_async([
            (AsyncMappedUser, "users"),
            (AsyncMappedPost, "posts"),
            (AsyncMappedComment, "comments")
        ], scenario_name)

    async def setup_profile_fixtures(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord]]:
        from rhosocial.activerecord.testsuite.feature.query.fixtures.async_models import AsyncUser, AsyncProfile
        return await self._setup_multiple_models_async([
            (AsyncUser, "users"),
            (AsyncProfile, "profiles"),
        ], scenario_name)

    async def setup_order_item_model(self, scenario_name: str) -> Type[AsyncActiveRecord]:
        """Set up the composite-PK AsyncOrderItem model for the query feature tests."""
        from rhosocial.activerecord.backend.impl.postgres import AsyncPostgresBackend
        from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType
        from providers.fixtures._common import drop_table
        from providers.fixtures.basic import TABLE_EXPRESSIONS as BASIC_EXPRS

        _, config = get_scenario(scenario_name)
        await AsyncCompositeOrderItemBase.configure(config, AsyncPostgresBackend)
        backend_instance = AsyncCompositeOrderItemBase.__backend__
        self._track_backend(backend_instance, self._active_async_backends)
        opts = ExecutionOptions(stmt_type=StatementType.DDL)
        try:
            sql, params = drop_table(backend_instance.dialect, "order_items").to_sql()
            await backend_instance.execute(sql, params, options=opts)
        except Exception:
            pass
        if fn := BASIC_EXPRS.get("order_items"):
            create_expr = fn(backend_instance.dialect, "order_items")
            sql, params = create_expr.to_sql()
            await backend_instance.execute(sql, params, options=opts)
        return AsyncCompositeOrderItemBase

    async def cleanup_after_test(self, scenario_name: str):
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
                except:  # noqa: E722
                    pass
        self._active_async_backends.clear()
