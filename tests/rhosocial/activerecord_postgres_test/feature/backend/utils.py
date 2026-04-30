# Test utilities for PostgreSQL extension tests.
"""Shared test utilities for PostgreSQL backend extension testing.

Extension integration tests use class-scoped fixtures with idempotent SQL
(CREATE TABLE IF NOT EXISTS + TRUNCATE RESTART IDENTITY). Each extension's
test file uses distinct table names, making them safe for file-level
parallel execution.

To run extension tests in parallel (file-level):
    pytest tests/.../extensions/ --dist=loadfile -n auto

Note: Do NOT use --dist=loadfile for the full test suite, as other tests
are not yet prepared for parallel execution. Only use it when running
extension tests specifically.
"""

import asyncio
import functools

import pytest


def _find_dialect(args, kwargs):
    """Find a dialect object from test function arguments."""
    for arg in args:
        if hasattr(arg, 'dialect'):
            return arg.dialect
    for val in kwargs.values():
        if hasattr(val, 'dialect'):
            return val.dialect
    return None


def _apply_to_methods(cls, decorator_factory):
    """Apply a decorator factory to all test methods in a class."""
    for attr_name in list(vars(cls)):
        if attr_name.startswith('test'):
            method = getattr(cls, attr_name)
            if callable(method):
                decorated = decorator_factory(method)
                setattr(cls, attr_name, decorated)
    return cls


def requires_extension(*extension_names, features=None):
    """Decorator to skip tests when specified PostgreSQL extensions are not installed.

    Supports decorating individual test functions, async test functions,
    and entire test classes (applies to each test method).

    Optionally checks specific extension features with version requirements.

    Args:
        *extension_names: Extension names that must be installed.
        features: Optional dict mapping extension name to list of feature names.
                  Each feature is checked via dialect.check_extension_feature().

    Usage:
        @requires_extension("hstore")
        def test_hstore(self, postgres_backend_single):
            ...

        @requires_extension("postgis")
        class TestPostgisIntegration:
            def test_geometry(self, postgres_backend_single):
                ...

        @requires_extension("vector", features={"vector": ["hnsw_index"]})
        def test_hnsw_index(self, postgres_backend_single):
            ...

        @requires_extension("postgis")
        async def test_postgis_async(self, async_postgres_backend_single):
            ...
    """
    def _check_extensions(dialect):
        for ext_name in extension_names:
            if not dialect.is_extension_installed(ext_name):
                pytest.skip(f"Extension '{ext_name}' not installed")
        if features:
            for ext_name, feature_list in features.items():
                for feature_name in feature_list:
                    if not dialect.check_extension_feature(ext_name, feature_name):
                        min_ver = dialect.get_extension_min_version_for_feature(ext_name, feature_name)
                        version_info = f" (requires >= {min_ver})" if min_ver else ""
                        pytest.skip(
                            f"Extension '{ext_name}' feature '{feature_name}' not supported{version_info}"
                        )

    def decorator(func_or_class):
        # When decorating a class, apply to each test method
        if isinstance(func_or_class, type):
            return _apply_to_methods(func_or_class, decorator)

        func = func_or_class

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            dialect = _find_dialect(args, kwargs)
            if dialect is None:
                pytest.skip("Cannot check extension availability: no backend/dialect available")
            _check_extensions(dialect)
            return func(*args, **kwargs)

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            dialect = _find_dialect(args, kwargs)
            if dialect is None:
                pytest.skip("Cannot check extension availability: no backend/dialect available")
            _check_extensions(dialect)
            return await func(*args, **kwargs)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return wrapper
    return decorator


def requires_extension_available(*extension_names):
    """Decorator to skip tests when specified PostgreSQL extensions are not available (installable).

    This checks pg_available_extensions, meaning the extension files exist on the server
    but may not yet be CREATE EXTENSION'd. Supports both synchronous and asynchronous tests,
    and class-level decoration.

    Usage:
        @requires_extension_available("postgis")
        def test_something(self, postgres_backend_single):
            ...

        @requires_extension_available("postgis")
        class TestPostgisAvailable:
            def test_something(self, postgres_backend_single):
                ...
    """
    def _check_available(dialect):
        for ext_name in extension_names:
            if not dialect.is_extension_available(ext_name):
                pytest.skip(f"Extension '{ext_name}' not available on this server")

    def decorator(func_or_class):
        # When decorating a class, apply to each test method
        if isinstance(func_or_class, type):
            return _apply_to_methods(func_or_class, decorator)

        func = func_or_class

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            dialect = _find_dialect(args, kwargs)
            if dialect is None:
                pytest.skip("Cannot check extension availability: no backend/dialect available")
            _check_available(dialect)
            return func(*args, **kwargs)

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            dialect = _find_dialect(args, kwargs)
            if dialect is None:
                pytest.skip("Cannot check extension availability: no backend/dialect available")
            _check_available(dialect)
            return await func(*args, **kwargs)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return wrapper
    return decorator


def skip_if_extension_missing(backend, *extension_names):
    """Check extension availability and skip if any is not installed.

    Intended for use in fixture setup to skip early before preparing test data.

    Args:
        backend: The backend instance (must have .dialect attribute).
        *extension_names: Extension names that must be installed.

    Raises:
        pytest.skip: If any required extension is not installed.
    """
    dialect = backend.dialect
    for ext_name in extension_names:
        if not dialect.is_extension_installed(ext_name):
            pytest.skip(f"Extension '{ext_name}' not installed")


def ensure_extension_installed(backend, *extension_names):
    """Try to create extension if available but not installed, then skip if still not installed.

    This function first checks if each extension is already installed.
    If not, it attempts to CREATE EXTENSION using expression objects.
    If the extension is not available or creation fails, the test is skipped.

    Intended for use in fixture setup for extensions that are available
    on the test server but not pre-installed.

    Args:
        backend: The backend instance (must have .dialect attribute).
        *extension_names: Extension names that must be installed.

    Raises:
        pytest.skip: If any required extension cannot be installed.
    """
    from rhosocial.activerecord.backend.impl.postgres.expression import (
        PostgresCreateExtensionExpression,
    )

    dialect = backend.dialect
    for ext_name in extension_names:
        if dialect.is_extension_installed(ext_name):
            continue
        if not dialect.is_extension_available(ext_name):
            pytest.skip(f"Extension '{ext_name}' not available on this server")
        try:
            create_ext = PostgresCreateExtensionExpression(
                dialect=dialect,
                name=ext_name,
            )
            sql, params = create_ext.to_sql()
            backend.execute(sql, params)
            backend.introspect_and_adapt()
        except Exception as e:
            pytest.skip(f"Could not create extension '{ext_name}': {e}")
        if not backend.dialect.is_extension_installed(ext_name):
            pytest.skip(f"Extension '{ext_name}' not installed after creation attempt")


async def async_ensure_extension_installed(backend, *extension_names):
    """Async version of ensure_extension_installed.

    Try to create extension if available but not installed, then skip if
    still not installed. Uses await for all backend operations.

    Args:
        backend: The async backend instance (must have .dialect attribute).
        *extension_names: Extension names that must be installed.

    Raises:
        pytest.skip: If any required extension cannot be installed.
    """
    from rhosocial.activerecord.backend.impl.postgres.expression import (
        PostgresCreateExtensionExpression,
    )

    dialect = backend.dialect
    for ext_name in extension_names:
        if dialect.is_extension_installed(ext_name):
            continue
        if not dialect.is_extension_available(ext_name):
            pytest.skip(f"Extension '{ext_name}' not available on this server")
        try:
            create_ext = PostgresCreateExtensionExpression(
                dialect=dialect,
                name=ext_name,
            )
            sql, params = create_ext.to_sql()
            await backend.execute(sql, params)
            await backend.introspect_and_adapt()
        except Exception as e:
            pytest.skip(f"Could not create extension '{ext_name}': {e}")
        if not backend.dialect.is_extension_installed(ext_name):
            pytest.skip(f"Extension '{ext_name}' not installed after creation attempt")
