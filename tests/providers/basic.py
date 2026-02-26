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
from typing import Type, List, Tuple

from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.backend.type_adapter import BaseSQLTypeAdapter
from rhosocial.activerecord.testsuite.feature.basic.interfaces import IBasicProvider
# ...and the scenarios are defined specifically for this backend.
from .scenarios import get_enabled_scenarios, get_scenario


# The models are defined generically in the testsuite...


class BasicProvider(IBasicProvider):
    """
    This is the postgres backend's implementation for the basic features test group.
    It connects the generic tests in the testsuite with the actual postgres database.
    """

    def __init__(self):
        self._active_backends = []
        self._active_async_backends = []

    def get_test_scenarios(self) -> List[str]:
        """Returns a list of names for all enabled scenarios for this backend."""
        return list(get_enabled_scenarios().keys())

    def _track_backend(self, backend_instance, collection: List) -> None:
        if backend_instance not in collection:
            collection.append(backend_instance)

    def _setup_model(self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str) -> Type[ActiveRecord]:
        """A generic helper method to handle the setup for any given model."""
        backend_class, config = get_scenario(scenario_name)
        model_class.configure(config, backend_class)

        backend_instance = model_class.__backend__
        self._track_backend(backend_instance, self._active_backends)

        self._reset_table_sync(model_class, table_name)
        return model_class

    async def _setup_async_model(self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str) -> Type[ActiveRecord]:
        """A generic helper method to handle the setup for any given async model."""
        from rhosocial.activerecord.backend.impl.postgres import AsyncPostgresBackend

        _, config = get_scenario(scenario_name)
        model_class.configure(config, AsyncPostgresBackend)

        backend_instance = model_class.__backend__
        self._track_backend(backend_instance, self._active_async_backends)

        await self._reset_table_async(model_class, table_name)
        return model_class

    def _reset_table_sync(self, model_class: Type[ActiveRecord], table_name: str) -> None:
        try:
            model_class.__backend__.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
        except Exception as e:
            print(f"Could not drop table {table_name}: {e}")

        schema_sql = self._load_postgres_schema(f"{table_name}.sql")
        model_class.__backend__.execute(schema_sql)

    async def _reset_table_async(self, model_class: Type[ActiveRecord], table_name: str) -> None:
        try:
            await model_class.__backend__.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
        except Exception as e:
            print(f"Could not drop table {table_name}: {e}")

        schema_sql = self._load_postgres_schema(f"{table_name}.sql")
        await model_class.__backend__.execute(schema_sql)

    def _initialize_model_schema(self, model_class: Type[ActiveRecord], table_name: str) -> None:
        """Initialize schema for a model that shares backend with another model."""
        self._reset_table_sync(model_class, table_name)

    async def _initialize_async_model_schema(self, model_class: Type[ActiveRecord], table_name: str) -> None:
        """Initialize schema for an async model that shares backend with another model."""
        await self._reset_table_async(model_class, table_name)

    def _setup_multiple_models(self, model_classes: List[Tuple[Type[ActiveRecord], str]], scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """Helper to set up multiple related models for a test, sharing a single backend."""
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
            result.append(model_class)

        return tuple(result)

    async def _setup_multiple_models_async(self, model_classes: List[Tuple[Type[ActiveRecord], str]], scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """Helper to set up multiple related async models for a test, sharing a single backend."""
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
            result.append(model_class)

        return tuple(result)

    # --- Implementation of the IBasicProvider interface ---

    def setup_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for user model tests."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import User
        return self._setup_model(User, scenario_name, "users")

    def setup_type_case_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for type case model tests."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import TypeCase
        return self._setup_model(TypeCase, scenario_name, "type_cases")

    def setup_type_test_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for type test model tests."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import TypeTestModel
        return self._setup_model(TypeTestModel, scenario_name, "type_tests")

    def setup_validated_field_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for validated field user model tests."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import ValidatedFieldUser
        return self._setup_model(ValidatedFieldUser, scenario_name, "validated_field_users")

    def setup_validated_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for validated user model tests."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import ValidatedUser
        return self._setup_model(ValidatedUser, scenario_name, "validated_users")

    def setup_mapped_models(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for MappedUser, MappedPost, and MappedComment models."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import MappedUser, MappedPost, MappedComment
        return self._setup_multiple_models([
            (MappedUser, "users"),
            (MappedPost, "posts"),
            (MappedComment, "comments")
        ], scenario_name)

    def setup_mixed_models(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """Sets up the database for ColumnMappingModel and MixedAnnotationModel."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import ColumnMappingModel
        from rhosocial.activerecord_postgres_test.feature.basic.fixtures.models import PostgresMixedAnnotationModel
        return self._setup_multiple_models([
            (ColumnMappingModel, "column_mapping_items"),
            (PostgresMixedAnnotationModel, "mixed_annotation_items")
        ], scenario_name)

    def setup_type_adapter_model_and_schema(self, scenario_name: str = None) -> Type[ActiveRecord]:
        """Sets up the database for the `TypeAdapterTest` model tests."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import TypeAdapterTest
        if scenario_name is None:
            scenario_name = self.get_test_scenarios()[0] if self.get_test_scenarios() else "default"
        return self._setup_model(TypeAdapterTest, scenario_name, "type_adapter_tests")

    async def setup_async_type_adapter_model_and_schema(self, scenario_name: str = None) -> Type[ActiveRecord]:
        """Sets up the database for the `AsyncTypeAdapterTest` model tests."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import AsyncTypeAdapterTest
        if scenario_name is None:
            scenario_name = self.get_test_scenarios()[0] if self.get_test_scenarios() else "default"
        return await self._setup_async_model(AsyncTypeAdapterTest, scenario_name, "type_adapter_tests")

    def get_yes_no_adapter(self) -> 'BaseSQLTypeAdapter':
        """Returns an instance of the YesOrNoBooleanAdapter."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import YesOrNoBooleanAdapter
        return YesOrNoBooleanAdapter()

    # --- Async implementations ---

    async def setup_async_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for async user model tests."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import AsyncUser
        return await self._setup_async_model(AsyncUser, scenario_name, "users")

    async def setup_async_type_case_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for async type case model tests."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import AsyncTypeCase
        return await self._setup_async_model(AsyncTypeCase, scenario_name, "type_cases")

    async def setup_async_type_test_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for async type test model tests."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import AsyncTypeTestModel
        return await self._setup_async_model(AsyncTypeTestModel, scenario_name, "type_tests")

    async def setup_async_validated_field_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for async validated field user model tests."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import AsyncValidatedFieldUser
        return await self._setup_async_model(AsyncValidatedFieldUser, scenario_name, "validated_field_users")

    async def setup_async_validated_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for async validated user model tests."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import AsyncValidatedUser
        return await self._setup_async_model(AsyncValidatedUser, scenario_name, "validated_users")

    async def setup_async_mapped_models(self, scenario_name: str) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        """Sets up the database for AsyncMappedUser, AsyncMappedPost, and AsyncMappedComment models."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import AsyncMappedUser, AsyncMappedPost, AsyncMappedComment
        return await self._setup_multiple_models_async([
            (AsyncMappedUser, "users"),
            (AsyncMappedPost, "posts"),
            (AsyncMappedComment, "comments")
        ], scenario_name)

    async def setup_async_mixed_models(self, scenario_name: str) -> Tuple[Type[ActiveRecord], ...]:
        """Sets up the database for AsyncColumnMappingModel and AsyncMixedAnnotationModel."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import AsyncColumnMappingModel
        from rhosocial.activerecord_postgres_test.feature.basic.fixtures.models import AsyncPostgresMixedAnnotationModel
        return await self._setup_multiple_models_async([
            (AsyncColumnMappingModel, "column_mapping_items"),
            (AsyncPostgresMixedAnnotationModel, "mixed_annotation_items")
        ], scenario_name)

    async def setup_async_type_adapter_model_and_schema(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for the `AsyncTypeAdapterTest` model tests."""
        from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import AsyncTypeAdapterTest
        return await self._setup_async_model(AsyncTypeAdapterTest, scenario_name, "type_adapter_tests")

    async def cleanup_after_test_async(self, scenario_name: str):
        """
        Performs async cleanup after a test, dropping all tables and disconnecting async backends.
        """
        tables_to_drop = [
            'users', 'type_cases', 'type_tests', 'validated_field_users',
            'validated_users', 'type_adapter_tests', 'posts', 'comments',
            'column_mapping_items', 'mixed_annotation_items'
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
        # Schemas are stored in the centralized location for basic feature.
        schema_dir = os.path.join(os.path.dirname(__file__), "..", "rhosocial", "activerecord_postgres_test", "feature", "basic", "schema")
        schema_path = os.path.join(schema_dir, filename)

        with open(schema_path, 'r', encoding='utf-8') as f:
            return f.read()

    def cleanup_after_test(self, scenario_name: str):
        """
        Performs cleanup after a test. This now iterates through the backends
        that were created during setup, drops tables, and explicitly disconnects them.
        """
        tables_to_drop = [
            'users', 'type_cases', 'type_tests', 'validated_field_users',
            'validated_users', 'type_adapter_tests', 'posts', 'comments',
            'column_mapping_items', 'mixed_annotation_items'
        ]
        for backend_instance in self._active_backends:
            try:
                # Drop all tables that might have been created for basic tests
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
