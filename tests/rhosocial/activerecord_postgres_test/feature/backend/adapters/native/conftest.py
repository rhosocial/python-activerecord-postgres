# tests/rhosocial/activerecord_postgres_test/feature/backend/adapters/native/conftest.py
"""Fixtures for the PostgreSQL native type tests.

The directory owns one logical database (``test_backend_adapters_native``),
so a test class opens one connection and creates its tables once.  Each module
declares the tables it needs in ``TABLES``; only those ``CREATE TABLE``
statements are executed from ``schema/native_types.sql``.  That keeps a
targeted run inside the remote-latency budget and keeps the statements shared
between modules in one visible place.

Extension tables live in ``schema/extension_types.sql`` and are only created
by a class fixture that has already established the extension exists.
"""

import re
from pathlib import Path

import pytest

SCHEMA_DIR = Path(__file__).parent / "schema"
BUILTIN_SCHEMA = SCHEMA_DIR / "native_types.sql"
EXTENSION_SCHEMA = SCHEMA_DIR / "extension_types.sql"


def _statements(path):
    """Return the non-empty statements of a small SQL script."""
    text = path.read_text(encoding="utf-8")
    text = re.sub(r"--[^\n]*", "", text)
    return [part.strip() for part in text.split(";") if part.strip()]


def _create_tables(backend, path, names):
    """Execute only the ``CREATE TABLE`` statements whose table is in ``names``."""
    for statement in _statements(path):
        for name in names:
            pattern = r"CREATE TABLE\s+(?:IF NOT EXISTS\s+)?{}\s*\(".format(
                re.escape(name)
            )
            if re.search(pattern, statement, re.IGNORECASE):
                backend.execute(statement)
                break


def _require_extension(backend, name):
    """Skip with the exact missing extension name when the server lacks it."""
    if backend.dialect.is_extension_installed(name):
        return
    available = "yes" if backend.dialect.is_extension_available(name) else "no"
    pytest.skip(
        "extension '{}' is not installed (pg_extension); "
        "pg_available_extensions says available: {}".format(name, available)
    )


def _supports_multirange(backend) -> bool:
    """PostgreSQL 14 added the multirange types; older servers refuse the DDL."""
    version = getattr(backend.dialect, "version", None)
    if version is None:
        return True
    return tuple(version)[:2] >= (14,)


@pytest.fixture(scope="class")
def native_backend(postgres_backend_class, request):
    """The class's backend, with the class's built-in tables created."""
    backend = postgres_backend_class
    names = tuple(getattr(request.cls, "TABLES", ()))
    if "native_ranges" in names and not _supports_multirange(backend):
        pytest.skip(
            "PostgreSQL before 14 has no multirange types "
            f"(server {backend.dialect.version})"
        )
    if names:
        _create_tables(backend, BUILTIN_SCHEMA, names)
    return backend


@pytest.fixture(scope="class")
def postgis_backend(postgres_backend_class):
    """Backend with the PostGIS table; skips when PostGIS is absent."""
    backend = postgres_backend_class
    _require_extension(backend, "postgis")
    _create_tables(backend, EXTENSION_SCHEMA, ("native_ext_postgis",))
    return backend


@pytest.fixture(scope="class")
def vector_backend(postgres_backend_class):
    """Backend with the pgvector table; skips when pgvector is absent."""
    backend = postgres_backend_class
    _require_extension(backend, "vector")
    _create_tables(backend, EXTENSION_SCHEMA, ("native_ext_pgvector",))
    return backend


@pytest.fixture(scope="class")
def hstore_backend(postgres_backend_class):
    """Backend with hstore installed and its table created.

    hstore is a contrib extension, so it can be created when the server lists
    it in ``pg_available_extensions``; the skip only happens when neither is
    true.
    """
    from rhosocial.activerecord_postgres_test.feature.backend.utils import (
        ensure_extension_installed,
    )

    backend = postgres_backend_class
    ensure_extension_installed(backend, "hstore")
    _create_tables(backend, EXTENSION_SCHEMA, ("native_ext_hstore",))
    return backend
