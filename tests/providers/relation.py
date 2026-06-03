# tests/providers/relation.py
import asyncio
from typing import Dict, List, Tuple, Type

from rhosocial.activerecord.model import ActiveRecord, AsyncActiveRecord
from rhosocial.activerecord.backend.impl.postgres import AsyncPostgresBackend
from rhosocial.activerecord.backend.impl.postgres.adapters.json import PostgresJSONBAdapter
from rhosocial.activerecord.testsuite.feature.relation.interfaces import IRelationProvider
from rhosocial.activerecord.testsuite.feature.relation.fixtures.models import (
    Employee, Department, Author, Book, Chapter, Profile,
    User, Post, Comment,
    AsyncUser, AsyncPost, AsyncComment,
    BoundaryOwner, BoundaryProfile, BoundaryPost,
    AsyncBoundaryOwner, AsyncBoundaryProfile, AsyncBoundaryPost,
)
from .scenarios import get_enabled_scenarios, get_scenario


EMPLOYEE_DEPARTMENT_SCHEMA = """
    DROP TABLE IF EXISTS employees CASCADE;
    DROP TABLE IF EXISTS departments CASCADE;
    CREATE TABLE departments (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT DEFAULT ''
    );
    CREATE TABLE employees (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL,
        department_id INTEGER NOT NULL
    );
"""

AUTHOR_BOOK_SCHEMA = """
    DROP TABLE IF EXISTS chapters CASCADE;
    DROP TABLE IF EXISTS books CASCADE;
    DROP TABLE IF EXISTS profiles CASCADE;
    DROP TABLE IF EXISTS authors CASCADE;
    CREATE TABLE authors (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    CREATE TABLE books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        author_id INTEGER NOT NULL
    );
    CREATE TABLE chapters (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        book_id INTEGER NOT NULL
    );
    CREATE TABLE profiles (
        id SERIAL PRIMARY KEY,
        bio TEXT NOT NULL,
        author_id INTEGER NOT NULL
    );
"""

USER_POST_COMMENT_SCHEMA = """
    DROP TABLE IF EXISTS comments CASCADE;
    DROP TABLE IF EXISTS posts CASCADE;
    DROP TABLE IF EXISTS users CASCADE;
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT,
        settings JSONB
    );
    CREATE TABLE posts (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        body TEXT NOT NULL,
        user_id INTEGER NOT NULL,
        view_count INTEGER NOT NULL DEFAULT 0,
        metadata JSONB
    );
    CREATE TABLE comments (
        id SERIAL PRIMARY KEY,
        body TEXT NOT NULL,
        post_id INTEGER NOT NULL,
        meta JSONB
    );
"""

RELATION_BOUNDARY_SCHEMA = """
    DROP TABLE IF EXISTS relation_boundary_posts CASCADE;
    DROP TABLE IF EXISTS relation_boundary_profiles CASCADE;
    DROP TABLE IF EXISTS relation_boundary_owners CASCADE;
    CREATE TABLE relation_boundary_owners (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    CREATE TABLE relation_boundary_profiles (
        id SERIAL PRIMARY KEY,
        bio TEXT NOT NULL,
        owner_id INTEGER NULL
    );
    CREATE TABLE relation_boundary_posts (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        owner_id INTEGER NULL
    );
"""


class RelationProvider(IRelationProvider):

    def __init__(self):
        self._active_backends = []
        self._active_async_backends = []
        self._sync_user_post_comment_setup = False
        self._async_user_post_comment_setup = False
        self._sync_relation_boundary_setup = False
        self._async_relation_boundary_setup = False

    def get_test_scenarios(self) -> List[str]:
        return list(get_enabled_scenarios().keys())

    def _execute_script(self, backend, sql: str):
        for statement in sql.split(";"):
            statement = statement.strip()
            if statement:
                backend.execute(statement)

    async def _execute_script_async(self, backend, sql: str):
        for statement in sql.split(";"):
            statement = statement.strip()
            if statement:
                await backend.execute(statement)

    def _configure_with_shared_backend(self, model_class, config, backend_class, backend):
        model_class.__connection_config__ = config
        model_class.__backend_class__ = backend_class
        model_class.__backend__ = backend

    def _configure_async_model_without_connection(self, model_class, config, backend=None):
        if backend is None:
            backend = AsyncPostgresBackend(connection_config=config)
        model_class.__connection_config__ = config
        model_class.__backend_class__ = AsyncPostgresBackend
        model_class.__backend__ = backend
        return backend

    def _configure_json_field_adapters(self):
        adapter = PostgresJSONBAdapter()
        User.__field_adapters__["settings"] = (adapter, str)
        Post.__field_adapters__["metadata"] = (adapter, str)
        Comment.__field_adapters__["meta"] = (adapter, str)
        AsyncUser.__field_adapters__["settings"] = (adapter, str)
        AsyncPost.__field_adapters__["metadata"] = (adapter, str)
        AsyncComment.__field_adapters__["meta"] = (adapter, str)

    def _setup_employee_department(self, scenario_name):
        backend_class, config = get_scenario(scenario_name)
        Employee.configure(config, backend_class)
        backend = Employee.backend()
        backend.connect()
        backend.introspect_and_adapt()
        self._active_backends.append(backend)
        self._execute_script(backend, EMPLOYEE_DEPARTMENT_SCHEMA)
        self._configure_with_shared_backend(Department, config, backend_class, backend)
        return Employee, Department

    def _setup_author_book(self, scenario_name):
        backend_class, config = get_scenario(scenario_name)
        Author.configure(config, backend_class)
        backend = Author.backend()
        backend.connect()
        backend.introspect_and_adapt()
        self._active_backends.append(backend)
        self._execute_script(backend, AUTHOR_BOOK_SCHEMA)
        self._configure_with_shared_backend(Book, config, backend_class, backend)
        self._configure_with_shared_backend(Chapter, config, backend_class, backend)
        self._configure_with_shared_backend(Profile, config, backend_class, backend)
        return Author, Book, Chapter, Profile

    def _setup_user_post_comment_sync(self, scenario_name):
        if not self._sync_user_post_comment_setup:
            self._configure_json_field_adapters()
            backend_class, config = get_scenario(scenario_name)
            User.configure(config, backend_class)
            backend = User.backend()
            backend.connect()
            backend.introspect_and_adapt()
            self._active_backends.append(backend)
            self._execute_script(backend, USER_POST_COMMENT_SCHEMA)
            self._configure_with_shared_backend(Post, config, backend_class, backend)
            self._configure_with_shared_backend(Comment, config, backend_class, backend)
            self._sync_user_post_comment_setup = True

    def _setup_user_post_comment_async(self, scenario_name):
        if not self._async_user_post_comment_setup:
            self._configure_json_field_adapters()
            _, config = get_scenario(scenario_name)
            backend = self._configure_async_model_without_connection(AsyncUser, config)
            self._configure_async_model_without_connection(AsyncPost, config, backend)
            self._configure_async_model_without_connection(AsyncComment, config, backend)
            self._active_async_backends.append(backend)
            self._async_user_post_comment_setup = True

    async def _ensure_user_post_comment_async_schema(self):
        backend = AsyncUser.backend()
        if backend not in self._active_async_backends:
            self._active_async_backends.append(backend)
        await backend.introspect_and_adapt()
        await self._execute_script_async(backend, USER_POST_COMMENT_SCHEMA)

    def _setup_relation_boundary_sync(self, scenario_name):
        if not self._sync_relation_boundary_setup:
            backend_class, config = get_scenario(scenario_name)
            BoundaryOwner.configure(config, backend_class)
            backend = BoundaryOwner.backend()
            backend.connect()
            backend.introspect_and_adapt()
            self._active_backends.append(backend)
            self._execute_script(backend, RELATION_BOUNDARY_SCHEMA)
            self._configure_with_shared_backend(
                BoundaryProfile,
                config,
                backend_class,
                backend,
            )
            self._configure_with_shared_backend(
                BoundaryPost,
                config,
                backend_class,
                backend,
            )
            self._sync_relation_boundary_setup = True

    async def _setup_relation_boundary_async(self, scenario_name):
        if not self._async_relation_boundary_setup:
            _, config = get_scenario(scenario_name)
            backend = self._configure_async_model_without_connection(
                AsyncBoundaryOwner,
                config,
            )
            self._configure_async_model_without_connection(
                AsyncBoundaryProfile,
                config,
                backend,
            )
            self._configure_async_model_without_connection(
                AsyncBoundaryPost,
                config,
                backend,
            )
            self._active_async_backends.append(backend)
            await backend.introspect_and_adapt()
            await self._execute_script_async(backend, RELATION_BOUNDARY_SCHEMA)
            self._async_relation_boundary_setup = True

    def setup_employee_department_fixtures(
        self,
        scenario_name: str,
    ) -> Tuple[Type[ActiveRecord], Type[ActiveRecord]]:
        return self._setup_employee_department(scenario_name)

    def setup_author_book_fixtures(
        self,
        scenario_name: str,
    ) -> Tuple[
        Type[ActiveRecord],
        Type[ActiveRecord],
        Type[ActiveRecord],
        Type[ActiveRecord],
    ]:
        return self._setup_author_book(scenario_name)

    def setup_user_model(self, scenario_name: str) -> Type[ActiveRecord]:
        self._setup_user_post_comment_sync(scenario_name)
        return User

    def setup_post_model(self, scenario_name: str) -> Type[ActiveRecord]:
        self._setup_user_post_comment_sync(scenario_name)
        return Post

    def setup_comment_model(self, scenario_name: str) -> Type[ActiveRecord]:
        self._setup_user_post_comment_sync(scenario_name)
        return Comment

    def setup_async_user_model(self, scenario_name: str) -> Type[AsyncActiveRecord]:
        self._setup_user_post_comment_async(scenario_name)
        return AsyncUser

    def setup_async_post_model(self, scenario_name: str) -> Type[AsyncActiveRecord]:
        self._setup_user_post_comment_async(scenario_name)
        return AsyncPost

    def setup_async_comment_model(self, scenario_name: str) -> Type[AsyncActiveRecord]:
        self._setup_user_post_comment_async(scenario_name)
        return AsyncComment

    def setup_relation_boundary_fixtures(
        self,
        scenario_name: str,
    ) -> Tuple[Type[ActiveRecord], Type[ActiveRecord], Type[ActiveRecord]]:
        self._setup_relation_boundary_sync(scenario_name)
        return BoundaryOwner, BoundaryProfile, BoundaryPost

    def setup_async_relation_boundary_fixtures(
        self,
        scenario_name: str,
    ) -> Tuple[
        Type[AsyncActiveRecord],
        Type[AsyncActiveRecord],
        Type[AsyncActiveRecord],
    ]:
        _, config = get_scenario(scenario_name)
        self._configure_async_model_without_connection(AsyncBoundaryOwner, config)
        self._configure_async_model_without_connection(
            AsyncBoundaryProfile,
            config,
            AsyncBoundaryOwner.backend(),
        )
        self._configure_async_model_without_connection(
            AsyncBoundaryPost,
            config,
            AsyncBoundaryOwner.backend(),
        )
        return AsyncBoundaryOwner, AsyncBoundaryProfile, AsyncBoundaryPost

    def load_relation_boundary_dataset(self, scenario_name: str, dataset_name: str) -> Dict[str, int]:
        self._setup_relation_boundary_sync(scenario_name)
        return self._load_relation_boundary_dataset(dataset_name)

    async def load_async_relation_boundary_dataset(
        self,
        scenario_name: str,
        dataset_name: str,
    ) -> Dict[str, int]:
        await self._setup_relation_boundary_async(scenario_name)
        return await self._load_async_relation_boundary_dataset(dataset_name)

    def _load_relation_boundary_dataset(self, dataset_name):
        if dataset_name == "null_foreign_key":
            profile = BoundaryProfile(bio="No owner", owner_id=None)
            profile.save()
            return {"profile_id": profile.id}

        if dataset_name == "orphan_foreign_key":
            missing_owner_id = 999999
            post = BoundaryPost(title="Orphan post", owner_id=missing_owner_id)
            post.save()
            return {"post_id": post.id, "missing_owner_id": missing_owner_id}

        if dataset_name == "owner_without_children":
            owner = BoundaryOwner(name="Owner without children")
            owner.save()
            return {"owner_id": owner.id}

        if dataset_name == "multiple_has_one_matches":
            owner = BoundaryOwner(name="Owner with duplicate profiles")
            owner.save()
            first = BoundaryProfile(bio="First profile", owner_id=owner.id)
            first.save()
            second = BoundaryProfile(bio="Second profile", owner_id=owner.id)
            second.save()
            return {
                "owner_id": owner.id,
                "first_profile_id": first.id,
                "second_profile_id": second.id,
            }

        raise ValueError(f"Unknown relation boundary dataset: {dataset_name}")

    async def _load_async_relation_boundary_dataset(self, dataset_name):
        if dataset_name == "null_foreign_key":
            profile = AsyncBoundaryProfile(bio="No owner", owner_id=None)
            await profile.save()
            return {"profile_id": profile.id}

        if dataset_name == "orphan_foreign_key":
            missing_owner_id = 999999
            post = AsyncBoundaryPost(title="Orphan post", owner_id=missing_owner_id)
            await post.save()
            return {"post_id": post.id, "missing_owner_id": missing_owner_id}

        if dataset_name == "owner_without_children":
            owner = AsyncBoundaryOwner(name="Owner without children")
            await owner.save()
            return {"owner_id": owner.id}

        if dataset_name == "multiple_has_one_matches":
            owner = AsyncBoundaryOwner(name="Owner with duplicate profiles")
            await owner.save()
            first = AsyncBoundaryProfile(bio="First profile", owner_id=owner.id)
            await first.save()
            second = AsyncBoundaryProfile(bio="Second profile", owner_id=owner.id)
            await second.save()
            return {
                "owner_id": owner.id,
                "first_profile_id": first.id,
                "second_profile_id": second.id,
            }

        raise ValueError(f"Unknown relation boundary dataset: {dataset_name}")

    def _reset_setup_state(self):
        self._sync_user_post_comment_setup = False
        self._async_user_post_comment_setup = False
        self._sync_relation_boundary_setup = False
        self._async_relation_boundary_setup = False

    def cleanup_after_test(self, scenario_name: str) -> None:
        for backend in self._active_backends:
            try:
                backend.disconnect()
            except Exception:
                pass
        self._active_backends.clear()
        for backend in self._active_async_backends:
            try:
                asyncio.run(backend.disconnect())
            except Exception:
                pass
        self._active_async_backends.clear()
        self._reset_setup_state()

    async def cleanup_after_test_async(self, scenario_name: str):
        for backend in self._active_backends:
            try:
                backend.disconnect()
            except Exception:
                pass
        self._active_backends.clear()
        for backend in self._active_async_backends:
            try:
                await backend.disconnect()
            except Exception:
                pass
        self._active_async_backends.clear()
        self._reset_setup_state()
