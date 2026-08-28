# tests/rhosocial/activerecord_postgres_test/feature/backend/examples_compile_test.py
"""Guard against syntax/API drift in the runnable examples.

Every script under ``examples/`` must at least compile. This test runs
without a database and fails fast if an example introduces a syntax error.
(API drift that only manifests at runtime is caught by ``scripts/run_examples.py``
against a live database in CI.)
"""
import py_compile
from pathlib import Path

import pytest

EXAMPLES_ROOT = (
    Path(__file__).resolve().parents[5]
    / "src" / "rhosocial" / "activerecord" / "backend" / "impl" / "postgres"
    / "examples"
)


def _collect_examples():
    if not EXAMPLES_ROOT.exists():
        return []
    return sorted(EXAMPLES_ROOT.rglob("*.py"))


def test_all_examples_compile():
    examples = _collect_examples()
    assert examples, f"no examples found under {EXAMPLES_ROOT}"
    failures = []
    for path in examples:
        try:
            py_compile.compile(str(path), doraise=True)
        except py_compile.PyCompileError as exc:  # pragma: no cover
            failures.append(str(exc))
    assert not failures, "example compile failures:\n" + "\n".join(failures)
