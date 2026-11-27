# tests/rhosocial/activerecord_postgres_test/feature/rules/backend/conftest.py
"""Fixtures for the postgres backend's formal rule suite.

The shared rule modules are star-imported into this package, so their fixtures
are re-exported here for pytest to resolve on the backend side. Every override
is derived from the *installed* backend package; no checkout or worktree path is
hardcoded. ``source_roots`` is deliberately a single pair: this repository owns
only its ``impl/postgres`` tree and never pairs it with another backend's tree.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import List, Tuple

import pytest

from rhosocial.activerecord.testsuite.feature.rules.backend import (
    test_no_raw_sql_in_src as raw_sql_rules,
    test_no_reflection_in_expression_layer as reflection_rules,
)
from rhosocial.activerecord.testsuite.feature.rules.backend.conftest import *  # noqa: F401,F403
from rhosocial.activerecord.testsuite.pytest_plugin import Declaration

DECLARATION = Declaration(
    uses_database=False,
    parallel=False,
    purpose="Executable assertions of the backend layer's formal constraints",
    passes_means="backend source obeys the mechanical and structural rule",
    fails_means="backend source introduces a forbidden construct or dependency",
)

BACKEND_NAME = "postgres"


def _python_files(root: Path) -> List[Path]:
    """Sorted Python files below *root*, excluding bytecode caches."""
    if not root.is_dir():
        return []
    return [path for path in sorted(root.rglob("*.py")) if "__pycache__" not in path.parts]


def expression_package() -> object:
    """The installed postgres expression package."""
    return importlib.import_module("rhosocial.activerecord.backend.impl.postgres.expression")


def impl_root() -> Path:
    """The installed ``.../backend/impl/postgres`` directory."""
    package = expression_package()
    for search_path in vars(package).get("__path__", ()):
        candidate = Path(search_path).resolve()
        for base in [candidate] + list(candidate.parents):
            if base.name == "impl" and base.parent.name == "backend":
                return (base / candidate.relative_to(base).parts[0]).resolve()
        if candidate.name in ("expression", "expression.py"):
            return candidate.parent.resolve()
    pytest.skip("postgres expression package has no discoverable impl root")


def project_root() -> Path:
    """Project root inferred from the installed postgres package layout."""
    for candidate in [impl_root()] + list(impl_root().parents):
        if (candidate / "src").is_dir() and (candidate / "tests").is_dir():
            return candidate
    pytest.skip("postgres project root is not discoverable from the installed package")


@pytest.fixture
def backend_expression_package() -> object:
    """The backend's expression package, discovered from the installed module."""
    return expression_package()


@pytest.fixture
def backend_dialect() -> object:
    """An instantiated ``PostgresDialect`` (no connection is opened)."""
    module = importlib.import_module("rhosocial.activerecord.backend.impl.postgres.dialect")
    return module.PostgresDialect()


@pytest.fixture
def backend_prefix() -> str:
    """The naming prefix postgres uses for its own declarations."""
    return "Postgres"


@pytest.fixture
def backend_impl_root() -> Path:
    """The single postgres-owned root used by file-driven shared rules."""
    return impl_root()


@pytest.fixture
def source_roots() -> List[Tuple[str, Path]]:
    """Only this backend's implementation tree, labelled with its own name."""
    return [(BACKEND_NAME, impl_root())]


@pytest.fixture
def backend_project_root() -> Path:
    """Project root inferred from the installed postgres package."""
    return project_root()


@pytest.fixture
def backend_test_files(backend_project_root) -> List[Path]:
    """Every Python test file in the postgres project."""
    files = _python_files(backend_project_root / "tests")
    assert files, f"no test files under {backend_project_root / 'tests'}"
    return files


@pytest.fixture
def backend_matrix_files(backend_test_files) -> List[Path]:
    """Round-trip matrices that declare an unconstructible pin list."""
    marker = "UN" + "CONSTRUCTIBLE"  # keep this fixture out of its own scan
    return [path for path in backend_test_files if marker in path.read_text(encoding="utf-8")]


# Reason-bearing grandfathered sites in this repository. The keys carry the
# ``postgres`` label published by ``source_roots``; the shared self-tests of the
# rule modules still use core-labelled keys, so the shipped defaults are merged
# below rather than replaced. Each reason names the non-expression object and
# the absent declared attribute that make reflection or a static SQL literal
# the only available spelling.

POSTGRES_REFLECTION_ENTRIES = {
    ("postgres", "adapters/neutral.py", "_decode_range_like", "getattr"): (
        5,
        "psycopg range attribute on an adapter; no declared expression field exposes a driver codec",
    ),
    ("postgres", "adapters/neutral.py", "_is_range_like", "hasattr"): (
        1,
        "psycopg range attribute on an adapter; no declared expression field exposes a driver codec",
    ),
    ("postgres", "backend/async_backend.py", "AsyncPostgresBackend", "getattr"): (
        5,
        "optional connection attribute on the async backend; no expression field models a live connection",
    ),
    ("postgres", "backend/async_backend.py", "AsyncPostgresBackend", "hasattr"): (
        3,
        "optional connection attribute on the async backend; no expression field models a live connection",
    ),
    ("postgres", "backend/backend.py", "PostgresBackend", "getattr"): (
        5,
        "optional connection attribute on the backend; no expression field models a live connection",
    ),
    ("postgres", "backend/backend.py", "PostgresBackend", "hasattr"): (
        2,
        "optional connection attribute on the backend; no expression field models a live connection",
    ),
    ("postgres", "backend/base.py", "PostgresBackendMixin", "getattr"): (
        6,
        "optional driver attribute on a backend mixin; the driver object declares no expression field",
    ),
    ("postgres", "backend/base.py", "PostgresConcurrencyMixin", "getattr"): (
        1,
        "optional driver attribute on a backend mixin; the driver object declares no expression field",
    ),
    ("postgres", "backend/base.py", "PostgresConcurrencyMixin", "hasattr"): (
        1,
        "optional driver attribute on a backend mixin; the driver object declares no expression field",
    ),
    ("postgres", "backend/sync.py", "PostgresBackend", "getattr"): (
        5,
        "optional connection attribute on the sync backend; no expression field models a live connection",
    ),
    ("postgres", "backend/sync.py", "PostgresBackend", "hasattr"): (
        2,
        "optional connection attribute on the sync backend; no expression field models a live connection",
    ),
    ("postgres", "cli/connection.py", "resolve_connection_config_from_args", "getattr"): (
        3,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/info.py", "check_protocol_support", "getattr"): (
        1,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/info.py", "check_protocol_support", "hasattr"): (
        1,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/info.py", "handle", "getattr"): (
        1,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/info.py", "handle", "hasattr"): (
        1,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/introspect.py", "_refuse_unused_arguments", "getattr"): (
        3,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/introspect.py", "_require_capability", "getattr"): (
        2,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/introspect.py", "_serialize_for_output", "hasattr"): (
        1,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/introspect.py", "handle", "getattr"): (
        1,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/named_connection.py", "handle", "getattr"): (
        2,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/named_expression.py", "_get_requested_version", "getattr"): (
        1,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/named_expression.py", "handle", "getattr"): (
        1,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/named_migration.py", "disconnect", "getattr"): (
        1,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/named_migration.py", "disconnect_async", "getattr"): (
        1,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/named_migration.py", "handle", "getattr"): (
        1,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/named_procedure.py", "handle", "getattr"): (
        1,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/named_procedure_graph.py", "handle", "getattr"): (
        1,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/query.py", "handle", "getattr"): (
        1,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/status.py", "_display_status_rich", "hasattr"): (
        8,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/status.py", "_serialize_for_output", "hasattr"): (
        1,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "cli/status.py", "handle", "getattr"): (
        1,
        "argparse namespace attribute on a CLI object; no declared expression field exists there",
    ),
    ("postgres", "column_types.py", "_require", "getattr"): (
        1,
        "optional driver attribute on a Python value; no declared expression field exists",
    ),
    ("postgres", "config.py", "PostgresConnectionConfig", "getattr"): (
        1,
        "optional setting on a config object; no declared expression field exists",
    ),
    ("postgres", "examples/introspection_objects.py", "_QuestionCounter", "getattr"): (
        1,
        "client-object call count in a shipped example; no declared expression field",
    ),
    ("postgres", "examples/schema_diff/array_type_diff.py", "<module>", "hasattr"): (
        1,
        "optional client capability in a shipped example; no declared expression field",
    ),
    ("postgres", "expression/ddl/partition.py", "PostgresPartitionValue", "getattr"): (
        1,
        "optional bound-value attribute on a value node; a plain value has no expression field",
    ),
    ("postgres", "expression/values.py", "_typed_element", "getattr"): (
        4,
        "optional attribute of a Python element in a helper; a bare int has no declared field",
    ),
    ("postgres", "introspection/introspector.py", "PostgreSQLIntrospectorMixin", "getattr"): (
        1,
        "optional catalog/driver attribute on an introspector; no declared expression field",
    ),
    ("postgres", "introspection/introspector.py", "PostgreSQLIntrospectorMixin", "hasattr"): (
        3,
        "optional catalog/driver attribute on an introspector; no declared expression field",
    ),
    (
        "postgres",
        "introspection/status_introspector.py",
        "AsyncPostgreSQLStatusIntrospector",
        "getattr",
    ): (2, "optional connection attribute on a status introspector; no expression field models it"),
    (
        "postgres",
        "introspection/status_introspector.py",
        "PostgreSQLStatusIntrospector",
        "getattr",
    ): (2, "optional connection attribute on a status introspector; no expression field models it"),
    (
        "postgres",
        "introspection/status_introspector.py",
        "PostgreSQLStatusIntrospectorMixin",
        "getattr",
    ): (4, "optional connection attribute on a status introspector; no expression field models it"),
    (
        "postgres",
        "introspection/status_introspector.py",
        "PostgreSQLStatusIntrospectorMixin",
        "hasattr",
    ): (4, "optional connection attribute on a status introspector; no expression field models it"),
    ("postgres", "introspection/type_parser.py", "PostgresTypeParser", "hasattr"): (
        1,
        "optional catalog attribute on a type parser; no declared expression field",
    ),
    ("postgres", "mixins/ddl/column.py", "PostgresAlterColumnModifierMixin", "getattr"): (
        6,
        "optional action attribute in a legacy formatter; core declares no PG-only field",
    ),
    ("postgres", "mixins/ddl/column.py", "PostgresAlterColumnModifierMixin", "hasattr"): (
        1,
        "optional action attribute in a legacy formatter; core declares no PG-only field",
    ),
    ("postgres", "mixins/ddl/constraint.py", "PostgresConstraintMixin", "getattr"): (
        2,
        "optional constraint attribute in a legacy formatter; core declares no PG-only field",
    ),
    ("postgres", "mixins/dml/copy.py", "PostgresCopyMixin", "getattr"): (
        1,
        "optional copy option in a formatter; the option object declares no expression field",
    ),
    ("postgres", "mixins/explain.py", "PostgresExplainMixin", "getattr"): (
        2,
        "optional statement flag in a legacy formatter; no declared expression field",
    ),
    ("postgres", "mixins/extension.py", "PostgresExtensionMixin", "hasattr"): (
        4,
        "optional extension capability on the dialect; a flag, not an expression field",
    ),
    ("postgres", "mixins/function.py", "PostgresFunctionMixin", "getattr"): (
        1,
        "optional function-call attribute in a legacy formatter; no declared field",
    ),
    ("postgres", "mixins/function.py", "PostgresFunctionMixin", "hasattr"): (
        1,
        "optional function-call attribute in a legacy formatter; no declared field",
    ),
    ("postgres", "mixins/introspection.py", "PostgresIntrospectionCapabilityMixin", "getattr"): (
        4,
        "optional dialect/driver capability probe; no declared expression field",
    ),
    ("postgres", "mixins/introspection.py", "PostgresIntrospectionCapabilityMixin", "hasattr"): (
        1,
        "optional dialect/driver capability probe; no declared expression field",
    ),
    ("postgres", "mixins/materialized_view.py", "PostgresMaterializedViewMixin", "getattr"): (
        1,
        "optional statement field in a legacy formatter; core declares no PG-only field",
    ),
    ("postgres", "mixins/property_graph_query.py", "PostgresPropertyGraphQueryMixin", "getattr"): (
        4,
        "optional graph override on the dialect; configuration, not an expression field",
    ),
    ("postgres", "mixins/transaction.py", "PostgresTransactionMixin", "hasattr"): (
        2,
        "optional transaction-object attribute on a backend; no expression field models it",
    ),
    ("postgres", "mixins/upsert.py", "PostgresUpsertMixin", "getattr"): (
        1,
        "optional upsert field in a legacy formatter; core declares no PG-only slot",
    ),
    ("postgres", "mixins/upsert.py", "PostgresUpsertMixin", "hasattr"): (
        1,
        "optional upsert field in a legacy formatter; core declares no PG-only slot",
    ),
    ("postgres", "partition/plan.py", "PostgresPartitionPlan", "getattr"): (
        1,
        "optional plan attribute on a non-expression plan; no expression field carries it",
    ),
    ("postgres", "storage_parameters.py", "validate_storage_parameters", "hasattr"): (
        1,
        "optional attribute on a parameter object; no declared expression field",
    ),
    ("postgres", "suggestion/_change_points.py", "declaration_params", "getattr"): (
        1,
        "optional schema-snapshot attribute in a helper; no declared expression field",
    ),
    ("postgres", "suggestion/_change_points.py", "projection", "getattr"): (
        1,
        "optional schema-snapshot attribute in a helper; no declared expression field",
    ),
    ("postgres", "suggestion/_change_points.py", "reusable_expression", "getattr"): (
        1,
        "optional schema-snapshot attribute in a helper; no declared expression field",
    ),
    ("postgres", "suggestion/_common.py", "PostgresPartitionSuggestionBase", "getattr"): (
        8,
        "optional declared attribute on a suggestion helper; no expression field to read",
    ),
    ("postgres", "suggestion/_common.py", "PostgresPartitionSuggestionBase", "hasattr"): (
        1,
        "optional declared attribute on a suggestion helper; no expression field to read",
    ),
    (
        "postgres",
        "suggestion/partition_add_suggestion.py",
        "PostgresPartitionAddSuggestionMixin",
        "getattr",
    ): (3, "optional catalog attribute in a legacy suggestion adapter; no expression field"),
    (
        "postgres",
        "suggestion/partition_drop_suggestion.py",
        "PostgresPartitionDropSuggestionMixin",
        "getattr",
    ): (3, "optional catalog attribute in a legacy suggestion adapter; no expression field"),
    (
        "postgres",
        "suggestion/partition_retention_suggestion.py",
        "PostgresPartitionRetentionSuggestionMixin",
        "getattr",
    ): (4, "optional catalog attribute in a legacy suggestion adapter; no expression field"),
    ("postgres", "suggestion/table_constraint_change_suggestion.py", "_live_by_name", "getattr"): (
        1,
        "optional schema-snapshot attribute in a formatter helper; no expression field",
    ),
    (
        "postgres",
        "suggestion/table_constraint_change_suggestion.py",
        "_spellable_constraint",
        "getattr",
    ): (2, "optional schema-snapshot attribute in a formatter helper; no expression field"),
    (
        "postgres",
        "suggestion/table_constraint_change_suggestion.py",
        "_spellable_constraint",
        "hasattr",
    ): (2, "optional schema-snapshot attribute in a formatter helper; no expression field"),
    ("postgres", "suggestion/table_index_change_suggestion.py", "_live_by_name", "getattr"): (
        1,
        "optional schema-snapshot attribute in a helper; no expression field",
    ),
}

POSTGRES_RAWSQL_ENTRIES = {
    ("postgres", "backend/base.py", "RawSQL"): (
        2,
        "strict-mode guard rejects caller-vouched RawSQL; no declared node carries arbitrary SQL text",
    ),
    ("postgres", "expression/ddl/partition.py", "RawSQL"): (
        1,
        "caller-vouched RawSQL bound value; no declared typed node carries arbitrary bound SQL text",
    ),
    ("postgres", "functions/ddl_index.py", "RawSQLPredicate"): (
        2,
        "caller-vouched index predicate; no declared predicate kind carries arbitrary SQL text",
    ),
    ("postgres", "functions/pgroonga.py", "RawSQL"): (
        3,
        "caller-vouched pgroonga operator text; no declared expression kind carries arbitrary text",
    ),
    ("postgres", "functions/pgvector.py", "RawSQL"): (
        2,
        "caller-vouched pgvector operator text; no declared expression kind carries arbitrary text",
    ),
}

POSTGRES_EXEC_ENTRIES = {
    ("postgres", "backend/async_backend.py", "get_server_version", "execute", "SELECT version()"): (
        1,
        "constant capability probe on a connection; no expression node or bind parameter exists",
    ),
    ("postgres", "backend/async_backend.py", "ping", "execute", "SELECT 1"): (
        1,
        "constant capability probe on a connection; no expression node or bind parameter exists",
    ),
    ("postgres", "backend/backend.py", "get_server_version", "execute", "SELECT version()"): (
        1,
        "constant capability probe on a connection; no expression node or bind parameter exists",
    ),
    ("postgres", "backend/backend.py", "ping", "execute", "SELECT 1"): (
        1,
        "constant capability probe on a connection; no expression node or bind parameter exists",
    ),
    ("postgres", "backend/sync.py", "get_server_version", "execute", "SELECT version()"): (
        1,
        "constant capability probe on a connection; no expression node or bind parameter exists",
    ),
    ("postgres", "backend/sync.py", "ping", "execute", "SELECT 1"): (
        1,
        "constant capability probe on a connection; no expression node or bind parameter exists",
    ),
    (
        "postgres",
        "examples/ddl/materialized_view.py",
        "<module>",
        "execute",
        "CREATE SCHEMA IF NOT EXISTS mv_reporting",
    ): (
        1,
        "fixed setup statement in a shipped example; no expression node or bind parameter exists",
    ),
    (
        "postgres",
        "examples/ddl/materialized_view.py",
        "<module>",
        "execute",
        "DROP SCHEMA IF EXISTS mv_reporting CASCA",
    ): (
        1,
        "fixed setup statement in a shipped example; no expression node or bind parameter exists",
    ),
}


@pytest.fixture
def reflection_allowlist():
    """Core's generic entries plus postgres' reason-bearing reflection sites."""
    return {**reflection_rules.REFLECTION_ALLOWLIST, **POSTGRES_REFLECTION_ENTRIES}


@pytest.fixture
def raw_sql_allowlist():
    """Core's generic entries plus postgres' caller-vouched RawSQL mentions."""
    return {**raw_sql_rules.RAWSQL_ALLOWLIST, **POSTGRES_RAWSQL_ENTRIES}


@pytest.fixture
def exec_allowlist():
    """Core's generic entries plus postgres' constant probes and examples."""
    return {**raw_sql_rules.EXEC_ALLOWLIST, **POSTGRES_EXEC_ENTRIES}
