"""
This file provides the concrete implementation of the `IBasicProvider` interface
that is defined in the `rhosocial-activerecord-testsuite` package.

Its main responsibilities are:
1.  Reporting which test scenarios (database configurations) are available.
2.  Setting up the database environment for a given test. This includes:
    - Getting the correct database configuration for the scenario.
    - Configuring the ActiveRecord model with a database connection.
    - Dropping any old tables and creating the necessary table schema.
3.  Cleaning up any resources after a test runs.
"""
import os
import sys
import logging
from typing import Type, List, Tuple, Optional, Set  # noqa: F401

logger = logging.getLogger(__name__)

from rhosocial.activerecord.model import ActiveRecord  # noqa: E402
from rhosocial.activerecord.backend.type_adapter import BaseSQLTypeAdapter  # noqa: E402
from rhosocial.activerecord.backend.options import ExecutionOptions, StatementType  # noqa: E402
from rhosocial.activerecord.testsuite.feature.basic.interfaces import IBasicSyncProvider, IBasicAsyncProvider  # noqa: E402
from rhosocial.activerecord.testsuite.core.protocols import WorkerTestProtocol  # noqa: E402

# Import the fixture selector utility
from rhosocial.activerecord.testsuite.utils import select_fixture  # noqa: E402

# Expression-based DDL fixtures
from providers.fixtures._common import drop_table
from providers.fixtures.basic import TABLE_EXPRESSIONS as BASIC_TABLE_EXPRESSIONS

# Import base version models (Python 3.8+)
from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import (  # noqa: E402
    User as UserBase, TypeCase as TypeCaseBase, ValidatedFieldUser as ValidatedFieldUserBase,
    TypeTestModel as TypeTestModelBase, ValidatedUser as ValidatedUserBase,
    TypeAdapterTest as TypeAdapterTestBase, YesOrNoBooleanAdapter,
    PydanticValidatedModel as PydanticValidatedModelBase,
    MappedUser as MappedUserBase, MappedPost as MappedPostBase, MappedComment as MappedCommentBase,
    ColumnMappingModel as ColumnMappingModelBase, MixedAnnotationModel as MixedAnnotationModelBase
)
from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import (  # noqa: E402
    BulkUser as BulkUserBase, AsyncBulkUser as AsyncBulkUserBase
)
# Import async base models
from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import (  # noqa: E402
    AsyncUser as AsyncUserBase, AsyncTypeCase as AsyncTypeCaseBase,
    AsyncValidatedUser as AsyncValidatedUserBase, AsyncValidatedFieldUser as AsyncValidatedFieldUserBase,
    AsyncTypeTestModel as AsyncTypeTestModelBase, AsyncTypeAdapterTest as AsyncTypeAdapterTestBase,
    AsyncPydanticValidatedModel as AsyncPydanticValidatedModelBase,
    AsyncMappedUser as AsyncMappedUserBase, AsyncMappedPost as AsyncMappedPostBase,
    AsyncMappedComment as AsyncMappedCommentBase,
    AsyncColumnMappingModel as AsyncColumnMappingModelBase, AsyncMixedAnnotationModel as AsyncMixedAnnotationModelBase
)

# Conditionally import Python 3.10+ models
User310 = TypeCase310 = ValidatedFieldUser310 = TypeTestModel310 = ValidatedUser310 = None
TypeAdapterTest310 = PydanticValidatedModel310 = MappedUser310 = MappedPost310 = MappedComment310 = None
ColumnMappingModel310 = MixedAnnotationModel310 = None
AsyncUser310 = AsyncTypeCase310 = AsyncValidatedFieldUser310 = AsyncTypeTestModel310 = None
AsyncValidatedUser310 = AsyncTypeAdapterTest310 = AsyncPydanticValidatedModel310 = AsyncMappedUser310 = AsyncMappedPost310 = None  # noqa: E501
AsyncMappedComment310 = AsyncColumnMappingModel310 = AsyncMixedAnnotationModel310 = None

if sys.version_info >= (3, 10):
    try:
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models_py310 import (
            User as User310, TypeCase as TypeCase310, ValidatedFieldUser as ValidatedFieldUser310,
            TypeTestModel as TypeTestModel310, ValidatedUser as ValidatedUser310,
            TypeAdapterTest as TypeAdapterTest310, PydanticValidatedModel as PydanticValidatedModel310,
            MappedUser as MappedUser310, MappedPost as MappedPost310, MappedComment as MappedComment310,
            ColumnMappingModel as ColumnMappingModel310, MixedAnnotationModel as MixedAnnotationModel310
        )
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models_py310 import (
            AsyncUser as AsyncUser310, AsyncTypeCase as AsyncTypeCase310,
            AsyncValidatedUser as AsyncValidatedUser310, AsyncValidatedFieldUser as AsyncValidatedFieldUser310,
            AsyncTypeTestModel as AsyncTypeTestModel310, AsyncTypeAdapterTest as AsyncTypeAdapterTest310,
            AsyncPydanticValidatedModel as AsyncPydanticValidatedModel310,
            AsyncMappedUser as AsyncMappedUser310, AsyncMappedPost as AsyncMappedPost310,
            AsyncMappedComment as AsyncMappedComment310,
            AsyncColumnMappingModel as AsyncColumnMappingModel310, AsyncMixedAnnotationModel as AsyncMixedAnnotationModel310  # noqa: E501
        )
    except ImportError as e:
        logger.warning(f"Failed to import Python 3.10+ fixtures: {e}")

# Conditionally import Python 3.11+ models
User311 = TypeCase311 = ValidatedFieldUser311 = TypeTestModel311 = ValidatedUser311 = None
TypeAdapterTest311 = PydanticValidatedModel311 = MappedUser311 = MappedPost311 = MappedComment311 = None
ColumnMappingModel311 = MixedAnnotationModel311 = None
AsyncUser311 = AsyncTypeCase311 = AsyncValidatedFieldUser311 = AsyncTypeTestModel311 = None
AsyncValidatedUser311 = AsyncTypeAdapterTest311 = AsyncPydanticValidatedModel311 = AsyncMappedUser311 = AsyncMappedPost311 = None  # noqa: E501
AsyncMappedComment311 = AsyncColumnMappingModel311 = AsyncMixedAnnotationModel311 = None

if sys.version_info >= (3, 11):
    try:
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models_py311 import (
            User as User311, TypeCase as TypeCase311, ValidatedFieldUser as ValidatedFieldUser311,
            TypeTestModel as TypeTestModel311, ValidatedUser as ValidatedUser311,
            TypeAdapterTest as TypeAdapterTest311, PydanticValidatedModel as PydanticValidatedModel311,
            MappedUser as MappedUser311, MappedPost as MappedPost311, MappedComment as MappedComment311,
            ColumnMappingModel as ColumnMappingModel311, MixedAnnotationModel as MixedAnnotationModel311
        )
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models_py311 import (
            AsyncUser as AsyncUser311, AsyncTypeCase as AsyncTypeCase311,
            AsyncValidatedUser as AsyncValidatedUser311, AsyncValidatedFieldUser as AsyncValidatedFieldUser311,
            AsyncTypeTestModel as AsyncTypeTestModel311, AsyncTypeAdapterTest as AsyncTypeAdapterTest311,
            AsyncPydanticValidatedModel as AsyncPydanticValidatedModel311,
            AsyncMappedUser as AsyncMappedUser311, AsyncMappedPost as AsyncMappedPost311,
            AsyncMappedComment as AsyncMappedComment311,
            AsyncColumnMappingModel as AsyncColumnMappingModel311, AsyncMixedAnnotationModel as AsyncMixedAnnotationModel311  # noqa: E501
        )
    except ImportError as e:
        logger.warning(f"Failed to import Python 3.11+ fixtures: {e}")

# Conditionally import Python 3.12+ models
User312 = TypeCase312 = ValidatedFieldUser312 = TypeTestModel312 = ValidatedUser312 = None
TypeAdapterTest312 = PydanticValidatedModel312 = MappedUser312 = MappedPost312 = MappedComment312 = None
ColumnMappingModel312 = MixedAnnotationModel312 = None
AsyncUser312 = AsyncTypeCase312 = AsyncValidatedFieldUser312 = AsyncTypeTestModel312 = None
AsyncValidatedUser312 = AsyncTypeAdapterTest312 = AsyncPydanticValidatedModel312 = AsyncMappedUser312 = AsyncMappedPost312 = None  # noqa: E501
AsyncMappedComment312 = AsyncColumnMappingModel312 = AsyncMixedAnnotationModel312 = None

if sys.version_info >= (3, 12):
    try:
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models_py312 import (
            User as User312, TypeCase as TypeCase312, ValidatedFieldUser as ValidatedFieldUser312,
            TypeTestModel as TypeTestModel312, ValidatedUser as ValidatedUser312,
            TypeAdapterTest as TypeAdapterTest312, PydanticValidatedModel as PydanticValidatedModel312,
            MappedUser as MappedUser312, MappedPost as MappedPost312, MappedComment as MappedComment312,
            ColumnMappingModel as ColumnMappingModel312, MixedAnnotationModel as MixedAnnotationModel312
        )
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models_py312 import (
            AsyncUser as AsyncUser312, AsyncTypeCase as AsyncTypeCase312,
            AsyncValidatedUser as AsyncValidatedUser312, AsyncValidatedFieldUser as AsyncValidatedFieldUser312,
            AsyncTypeTestModel as AsyncTypeTestModel312, AsyncTypeAdapterTest as AsyncTypeAdapterTest312,
            AsyncPydanticValidatedModel as AsyncPydanticValidatedModel312,
            AsyncMappedUser as AsyncMappedUser312, AsyncMappedPost as AsyncMappedPost312,
            AsyncMappedComment as AsyncMappedComment312,
            AsyncColumnMappingModel as AsyncColumnMappingModel312, AsyncMixedAnnotationModel as AsyncMixedAnnotationModel312  # noqa: E501
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
TypeCase = _select_model_class(TypeCaseBase, TypeCase312, TypeCase311, TypeCase310, "TypeCase")
ValidatedFieldUser = _select_model_class(ValidatedFieldUserBase, ValidatedFieldUser312, ValidatedFieldUser311, ValidatedFieldUser310, "ValidatedFieldUser")  # noqa: E501
TypeTestModel = _select_model_class(TypeTestModelBase, TypeTestModel312, TypeTestModel311, TypeTestModel310, "TypeTestModel")  # noqa: E501
ValidatedUser = _select_model_class(ValidatedUserBase, ValidatedUser312, ValidatedUser311, ValidatedUser310, "ValidatedUser")  # noqa: E501
TypeAdapterTest = _select_model_class(TypeAdapterTestBase, TypeAdapterTest312, TypeAdapterTest311, TypeAdapterTest310, "TypeAdapterTest")  # noqa: E501
PydanticValidatedModel = _select_model_class(PydanticValidatedModelBase, PydanticValidatedModel312, PydanticValidatedModel311, PydanticValidatedModel310, "PydanticValidatedModel")  # noqa: E501
MappedUser = _select_model_class(MappedUserBase, MappedUser312, MappedUser311, MappedUser310, "MappedUser")
MappedPost = _select_model_class(MappedPostBase, MappedPost312, MappedPost311, MappedPost310, "MappedPost")
MappedComment = _select_model_class(MappedCommentBase, MappedComment312, MappedComment311, MappedComment310, "MappedComment")  # noqa: E501
ColumnMappingModel = _select_model_class(ColumnMappingModelBase, ColumnMappingModel312, ColumnMappingModel311, ColumnMappingModel310, "ColumnMappingModel")  # noqa: E501
MixedAnnotationModel = _select_model_class(MixedAnnotationModelBase, MixedAnnotationModel312, MixedAnnotationModel311, MixedAnnotationModel310, "MixedAnnotationModel")  # noqa: E501

# Select async models
AsyncUser = _select_model_class(AsyncUserBase, AsyncUser312, AsyncUser311, AsyncUser310, "AsyncUser")
AsyncTypeCase = _select_model_class(AsyncTypeCaseBase, AsyncTypeCase312, AsyncTypeCase311, AsyncTypeCase310, "AsyncTypeCase")  # noqa: E501
AsyncValidatedFieldUser = _select_model_class(AsyncValidatedFieldUserBase, AsyncValidatedFieldUser312, AsyncValidatedFieldUser311, AsyncValidatedFieldUser310, "AsyncValidatedFieldUser")  # noqa: E501
AsyncTypeTestModel = _select_model_class(AsyncTypeTestModelBase, AsyncTypeTestModel312, AsyncTypeTestModel311, AsyncTypeTestModel310, "AsyncTypeTestModel")  # noqa: E501
AsyncValidatedUser = _select_model_class(AsyncValidatedUserBase, AsyncValidatedUser312, AsyncValidatedUser311, AsyncValidatedUser310, "AsyncValidatedUser")  # noqa: E501
AsyncTypeAdapterTest = _select_model_class(AsyncTypeAdapterTestBase, AsyncTypeAdapterTest312, AsyncTypeAdapterTest311, AsyncTypeAdapterTest310, "AsyncTypeAdapterTest")  # noqa: E501
AsyncPydanticValidatedModel = _select_model_class(AsyncPydanticValidatedModelBase, AsyncPydanticValidatedModel312, AsyncPydanticValidatedModel311, AsyncPydanticValidatedModel310, "AsyncPydanticValidatedModel")  # noqa: E501
AsyncMappedUser = _select_model_class(AsyncMappedUserBase, AsyncMappedUser312, AsyncMappedUser311, AsyncMappedUser310, "AsyncMappedUser")  # noqa: E501
AsyncMappedPost = _select_model_class(AsyncMappedPostBase, AsyncMappedPost312, AsyncMappedPost311, AsyncMappedPost310, "AsyncMappedPost")  # noqa: E501
AsyncMappedComment = _select_model_class(AsyncMappedCommentBase, AsyncMappedComment312, AsyncMappedComment311, AsyncMappedComment310, "AsyncMappedComment")  # noqa: E501
AsyncColumnMappingModel = _select_model_class(AsyncColumnMappingModelBase, AsyncColumnMappingModel312, AsyncColumnMappingModel311, AsyncColumnMappingModel310, "AsyncColumnMappingModel")  # noqa: E501
AsyncMixedAnnotationModel = _select_model_class(AsyncMixedAnnotationModelBase, AsyncMixedAnnotationModel312, AsyncMixedAnnotationModel311, AsyncMixedAnnotationModel310, "AsyncMixedAnnotationModel")  # noqa: E501
BulkUser = BulkUserBase
AsyncBulkUser = AsyncBulkUserBase

# ...and the scenarios are defined specifically for this backend.
from .scenarios import get_enabled_scenarios, get_scenario  # noqa: E402


class BasicProviderBase:
    def __init__(self):
        self._scenario_db_files = {}
        self._created_tables: Set[str] = set()

    def get_test_scenarios(self) -> List[str]:
        return list(get_enabled_scenarios().keys())

    def get_yes_no_adapter(self) -> 'BaseSQLTypeAdapter':
        return YesOrNoBooleanAdapter()

    def _track_backend(self, backend_instance, collection) -> None:
        if backend_instance not in collection:
            collection.append(backend_instance)

    def _load_postgres_schema(self, filename: str) -> str:
        schema_dir = os.path.join(
            os.path.dirname(__file__), "..", "rhosocial", "activerecord_postgres_test", "feature", "basic", "schema"
        )
        schema_path = os.path.join(schema_dir, filename)
        with open(schema_path, 'r', encoding='utf-8') as f:
            return f.read()


class BasicSyncProvider(BasicProviderBase, IBasicSyncProvider, WorkerTestProtocol):

    def __init__(self):
        super().__init__()
        self._active_backends = []

    def _setup_model(self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str) -> Type[ActiveRecord]:
        backend_class, config = get_scenario(scenario_name)
        model_class.configure(config, backend_class)
        backend_instance = model_class.__backend__
        self._track_backend(backend_instance, self._active_backends)
        self._reset_table_sync(model_class, table_name)
        self._created_tables.add(table_name)
        return model_class

    def _reset_table_sync(self, model_class: Type[ActiveRecord], table_name: str) -> None:
        opts = ExecutionOptions(stmt_type=StatementType.DDL)
        try:
            sql, params = drop_table(model_class.__backend__.dialect, table_name).to_sql()
            model_class.__backend__.execute(sql, params, options=opts)
        except Exception as e:
            print(f"Could not drop table {table_name}: {e}")
        if fn := BASIC_TABLE_EXPRESSIONS.get(table_name):
            create_expr = fn(model_class.__backend__.dialect, table_name)
            sql, params = create_expr.to_sql()
            model_class.__backend__.execute(sql, params, options=opts)

    def _initialize_model_schema(self, model_class: Type[ActiveRecord], table_name: str) -> None:
        self._reset_table_sync(model_class, table_name)

    def _setup_multiple_models(self, model_classes: List[Tuple[Type[ActiveRecord], str]], scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        if not model_classes:
            return tuple()
        first_model_class, first_table_name = model_classes[0]
        first_model = self._setup_model(first_model_class, scenario_name, first_table_name)
        shared_backend = first_model.__backend__
        result = [first_model]
        for model_class, table_name in model_classes[1:]:
            model_class.__connection_config__ = first_model.__connection_config__
            model_class.__backend_class__ = first_model.__backend_class__
            model_class.__backend__ = shared_backend
            self._track_backend(shared_backend, self._active_backends)
            self._initialize_model_schema(model_class, table_name)
            self._created_tables.add(table_name)
            result.append(model_class)
        return tuple(result)

    def setup_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return self._setup_model(User, scenario_name, "users")

    def setup_type_case_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return self._setup_model(TypeCase, scenario_name, "type_cases")

    def setup_type_test_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return self._setup_model(TypeTestModel, scenario_name, "type_tests")

    def setup_validated_field_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return self._setup_model(ValidatedFieldUser, scenario_name, "validated_field_users")

    def setup_validated_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return self._setup_model(ValidatedUser, scenario_name, "validated_users")

    def setup_pydantic_validated_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return self._setup_model(PydanticValidatedModel, scenario_name, "pydantic_validated_models")

    def setup_mapped_models(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        return self._setup_multiple_models([
            (MappedUser, "users"),
            (MappedPost, "posts"),
            (MappedComment, "comments")
        ], scenario_name)

    def setup_mixed_models(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        from rhosocial.activerecord_postgres_test.feature.basic.fixtures.models import PostgresMixedAnnotationModel
        return self._setup_multiple_models([
            (ColumnMappingModel, "column_mapping_items"),
            (PostgresMixedAnnotationModel, "mixed_annotation_items")
        ], scenario_name)

    def setup_type_adapter_model_and_schema(self, scenario_name: str = None) -> Type[ActiveRecord]:
        if scenario_name is None:
            scenario_name = self.get_test_scenarios()[0] if self.get_test_scenarios() else "default"
        return self._setup_model(TypeAdapterTest, scenario_name, "type_adapter_tests")

    def setup_bulk_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return self._setup_model(BulkUser, scenario_name, "bulk_users")

    def setup_order_item_model(self, scenario_name: str) -> Type[ActiveRecord]:
        raise NotImplementedError

    def setup_order_model(self, scenario_name: str) -> Type[ActiveRecord]:
        raise NotImplementedError

    def setup_mapped_order_item_model(self, scenario_name: str) -> Type[ActiveRecord]:
        raise NotImplementedError

    def setup_product_model(self, scenario_name: str) -> Type[ActiveRecord]:
        raise NotImplementedError

    def setup_product_form_a_model(self, scenario_name: str) -> Type[ActiveRecord]:
        raise NotImplementedError

    def setup_product_with_proxy_model(self, scenario_name: str) -> Type[ActiveRecord]:
        raise NotImplementedError

    def setup_product_with_column_and_adapter_model(self, scenario_name: str) -> Type[ActiveRecord]:
        raise NotImplementedError

    def get_worker_connection_params(self, scenario_name: str, fixture_type: str = None) -> dict:
        from .scenarios import SCENARIO_MAP
        is_async = fixture_type and fixture_type.startswith('async_')
        backend_class_name = 'AsyncPostgresBackend' if is_async else 'PostgresBackend'
        table_name = 'users'
        if fixture_type:
            base_type = fixture_type.replace('async_', '')
            table_map = {
                'user': 'users',
                'type_case': 'type_cases',
                'type_test': 'type_tests',
                'validated_field_user': 'validated_field_users',
                'validated_user': 'validated_users',
                'pydantic_validated_model': 'pydantic_validated_models',
                'type_adapter_test': 'type_adapter_tests',
            }
            table_name = table_map.get(base_type, 'users')
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
            'schema_sql': self._load_postgres_schema(f'{table_name}.sql'),
        }

    def get_worker_schema_sql(self, scenario_name: str, table_name: str) -> str:
        return self._load_postgres_schema(f'{table_name}.sql')

    def cleanup_after_test(self, scenario_name: str):
        for backend_instance in self._active_backends:
            try:
                for table_name in list(self._created_tables):
                    try:
                        backend_instance.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
                    except Exception:
                        pass
            finally:
                try:
                    backend_instance.disconnect()
                except Exception:
                    pass
        self._active_backends.clear()
        self._created_tables.clear()


class BasicAsyncProvider(BasicProviderBase, IBasicAsyncProvider):

    def __init__(self):
        super().__init__()
        self._active_async_backends = []

    async def _setup_async_model(self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str) -> Type[ActiveRecord]:
        from rhosocial.activerecord.backend.impl.postgres import AsyncPostgresBackend
        _, config = get_scenario(scenario_name)
        await model_class.configure(config, AsyncPostgresBackend)
        backend_instance = model_class.__backend__
        self._track_backend(backend_instance, self._active_async_backends)
        await self._reset_table_async(model_class, table_name)
        self._created_tables.add(table_name)
        return model_class

    async def _reset_table_async(self, model_class: Type[ActiveRecord], table_name: str) -> None:
        opts = ExecutionOptions(stmt_type=StatementType.DDL)
        try:
            sql, params = drop_table(model_class.__backend__.dialect, table_name).to_sql()
            await model_class.__backend__.execute(sql, params, options=opts)
        except Exception as e:
            print(f"Could not drop table {table_name}: {e}")
        if fn := BASIC_TABLE_EXPRESSIONS.get(table_name):
            create_expr = fn(model_class.__backend__.dialect, table_name)
            sql, params = create_expr.to_sql()
            await model_class.__backend__.execute(sql, params, options=opts)

    async def _initialize_async_model_schema(self, model_class: Type[ActiveRecord], table_name: str) -> None:
        await self._reset_table_async(model_class, table_name)

    async def _setup_multiple_models_async(self, model_classes: List[Tuple[Type[ActiveRecord], str]], scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        if not model_classes:
            return tuple()
        first_model_class, first_table_name = model_classes[0]
        first_model = await self._setup_async_model(first_model_class, scenario_name, first_table_name)
        shared_backend = first_model.__backend__
        result = [first_model]
        for model_class, table_name in model_classes[1:]:
            model_class.__connection_config__ = first_model.__connection_config__
            model_class.__backend_class__ = first_model.__backend_class__
            model_class.__backend__ = shared_backend
            self._track_backend(shared_backend, self._active_async_backends)
            await self._initialize_async_model_schema(model_class, table_name)
            self._created_tables.add(table_name)
            result.append(model_class)
        return tuple(result)

    async def setup_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return await self._setup_async_model(AsyncUser, scenario_name, "users")

    async def setup_type_case_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return await self._setup_async_model(AsyncTypeCase, scenario_name, "type_cases")

    async def setup_type_test_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return await self._setup_async_model(AsyncTypeTestModel, scenario_name, "type_tests")

    async def setup_validated_field_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return await self._setup_async_model(AsyncValidatedFieldUser, scenario_name, "validated_field_users")

    async def setup_validated_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return await self._setup_async_model(AsyncValidatedUser, scenario_name, "validated_users")

    async def setup_pydantic_validated_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return await self._setup_async_model(AsyncPydanticValidatedModel, scenario_name, "pydantic_validated_models")

    async def setup_mapped_models(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        return await self._setup_multiple_models_async([
            (AsyncMappedUser, "users"),
            (AsyncMappedPost, "posts"),
            (AsyncMappedComment, "comments")
        ], scenario_name)

    async def setup_mixed_models(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        from rhosocial.activerecord_postgres_test.feature.basic.fixtures.models import AsyncPostgresMixedAnnotationModel
        return await self._setup_multiple_models_async([
            (AsyncColumnMappingModel, "column_mapping_items"),
            (AsyncPostgresMixedAnnotationModel, "mixed_annotation_items")
        ], scenario_name)

    async def setup_type_adapter_model_and_schema(self, scenario_name: str) -> Type[ActiveRecord]:
        return await self._setup_async_model(AsyncTypeAdapterTest, scenario_name, "type_adapter_tests")

    async def setup_bulk_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        return await self._setup_async_model(AsyncBulkUser, scenario_name, "bulk_users")

    async def setup_order_item_model(self, scenario_name: str) -> Type[ActiveRecord]:
        raise NotImplementedError

    async def setup_order_model(self, scenario_name: str) -> Type[ActiveRecord]:
        raise NotImplementedError

    async def setup_mapped_order_item_model(self, scenario_name: str) -> Type[ActiveRecord]:
        raise NotImplementedError

    async def setup_product_model(self, scenario_name: str) -> Type[ActiveRecord]:
        raise NotImplementedError

    async def setup_product_form_a_model(self, scenario_name: str) -> Type[ActiveRecord]:
        raise NotImplementedError

    async def setup_product_with_proxy_model(self, scenario_name: str) -> Type[ActiveRecord]:
        raise NotImplementedError

    async def setup_product_with_column_and_adapter_model(self, scenario_name: str) -> Type[ActiveRecord]:
        raise NotImplementedError

    async def cleanup_after_test(self, scenario_name: str):
        for backend_instance in self._active_async_backends:
            try:
                for table_name in list(self._created_tables):
                    try:
                        await backend_instance.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
                    except Exception:
                        pass
            finally:
                try:
                    await backend_instance.disconnect()
                except Exception:
                    pass
        self._active_async_backends.clear()
        self._created_tables.clear()
