# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/dialect/test_artifact_registry.py
"""The PostgreSQL backend's artifact registry: what it owns and what it refuses.

Deserialization is a whitelist lookup, not an import driven by the artifact
(expression rules, untrusted artifacts). The table a read resolves against is
decided by its owner: ``dialect.artifact_registry()`` for the kinds a backend
can read, which for PostgreSQL is ``backend_registry()`` -- core's kinds plus
the ``postgres_*`` kinds its expression package declares, frozen and cached.

These tests pin the four properties that make that table trustworthy:

* a PostgreSQL-proprietary kind resolves, through the dialect;
* another backend's kind does not -- a kind resolves to exactly one owner's
  class and there is no cross-backend merge anywhere;
* the published table is frozen, so one reader cannot change what another
  resolves;
* a default ``deserialize(..., dialect)`` picks that table without the caller
  naming it, which is what keeps existing call sites working.
"""

import pytest

from rhosocial.activerecord.backend.expression.registry import (
    RegistryError,
    RegistryReason,
)
from rhosocial.activerecord.backend.expression.serialization import deserialize, serialize
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression import (
    PostgresCopyFromStatement,
    PostgresCopyToStatement,
    PostgresCreateMaterializedViewExpression,
    PostgresCreateTableAsStatement,
    PostgresDeleteStatement,
    PostgresExplainStatement,
    PostgresInsertStatement,
    PostgresQueryStatement,
    PostgresRefreshMaterializedViewStatement,
    PostgresUpdateStatement,
)
from rhosocial.activerecord.backend.impl.postgres.registry import backend_registry


@pytest.fixture
def dialect():
    return PostgresDialect(version=(19, 0, 0))


def _copy_statement(dialect):
    return PostgresCopyToStatement(dialect, table_name="users")


def test_dialect_registry_resolves_postgres_proprietary_kinds(dialect):
    """COPY is this backend's own kind."""
    registry = dialect.artifact_registry()
    for cls in (
        PostgresCopyToStatement,
        PostgresCopyFromStatement,
    ):
        resolved = registry.resolve_node(cls.kind())
        assert resolved is cls, (
            f"{cls.kind()} resolved to {resolved.__module__}.{resolved.__name__}, "
            f"not to the class that declares it"
        )


def test_a_hint_payload_is_not_a_registered_kind(dialect):
    """A hint is a value carried by a statement, never a kind of its own.

    The payload is a plain dictionary now (there is no payload class at all),
    so the check is that no ``postgres_hint`` kind exists to resolve.
    """
    with pytest.raises(RegistryError) as excinfo:
        dialect.artifact_registry().resolve_node("postgres_hint")
    assert excinfo.value.reason is RegistryReason.KIND_NOT_REGISTERED


def test_the_eight_hinted_statement_kinds_resolve(dialect):
    """The hinted statements are PostgreSQL's kinds, owned by this backend.

    Six extend a core class and live in the hinted-statements module; two were
    already PostgreSQL-derived materialized-view statements and were extended
    in place. Either way each is declared by this backend's expression package,
    so the dialect's table must resolve its own kind without the caller naming
    the registry.
    """
    registry = dialect.artifact_registry()
    for cls in (
        PostgresQueryStatement,
        PostgresInsertStatement,
        PostgresUpdateStatement,
        PostgresDeleteStatement,
        PostgresExplainStatement,
        PostgresCreateTableAsStatement,
        PostgresCreateMaterializedViewExpression,
        PostgresRefreshMaterializedViewStatement,
    ):
        resolved = registry.resolve_node(cls.kind())
        assert resolved is cls, (
            f"{cls.kind()} resolved to {resolved.__module__}.{resolved.__name__}, "
            f"not to the class that declares it"
        )


def test_dialect_registry_carries_core_kinds_too(dialect):
    """The overlap with core is composition, not a second copy or a second owner."""
    from rhosocial.activerecord.backend.expression.core import Literal

    assert dialect.artifact_registry().resolve_node(Literal.kind()) is Literal


def test_dialect_registry_refuses_another_backends_kind(dialect):
    """A SQLite kind is not in PostgreSQL's table, and the refusal names it.

    The dialect's table is built with ``base=core_registry()`` and its own
    package -- it never merges another backend's kinds, so an artifact naming
    one is a kind this build cannot read rather than a class it imports.
    """
    from rhosocial.activerecord.backend.impl.sqlite.expression.attach import (
        SQLiteAttachStatement,
    )

    foreign_kind = SQLiteAttachStatement.kind()
    with pytest.raises(RegistryError) as excinfo:
        dialect.artifact_registry().resolve_node(foreign_kind)
    assert excinfo.value.reason is RegistryReason.KIND_NOT_REGISTERED
    assert excinfo.value.kind == foreign_kind


def test_backend_registry_is_frozen_and_cached():
    """The published table is a value: cached, frozen, and refuses mutation."""
    registry = backend_registry()
    assert registry is backend_registry(), "backend_registry() rebuilt a second table"
    assert registry.is_frozen

    with pytest.raises(RegistryError) as excinfo:
        registry.register_node(PostgresCopyToStatement)
    assert excinfo.value.reason is RegistryReason.REGISTRY_FROZEN


def test_deserialize_selects_the_dialects_registry_by_default(dialect):
    """``deserialize(spec, dialect)`` reads PostgreSQL kinds without a hint.

    Selection order is explicit ``registry`` -> ``dialect.artifact_registry()``
    -> ``core_registry()``. The COPY statement is not core's, so the round trip
    only succeeds when the dialect supplies its own table.
    """
    original = _copy_statement(dialect)
    restored = deserialize(serialize(original), dialect)
    assert isinstance(restored, PostgresCopyToStatement)
    assert restored.to_sql() == original.to_sql()


def test_matrix_kinds_roundtrip_through_the_backend_registry(dialect):
    """A COPY tree round-trips through the frozen table directly.

    Passing ``backend_registry()`` explicitly is the other half of the same
    contract: the reads in this repository's matrix name the table rather than
    registering their kinds into a process-global one.
    """
    original = _copy_statement(dialect)
    spec = serialize(original)
    restored = deserialize(spec, dialect, registry=dialect.artifact_registry())
    assert isinstance(restored, PostgresCopyToStatement)
    assert restored.to_sql() == original.to_sql()
