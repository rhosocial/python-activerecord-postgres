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
