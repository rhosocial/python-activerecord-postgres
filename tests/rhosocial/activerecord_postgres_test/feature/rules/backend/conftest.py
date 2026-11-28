# tests/rhosocial/activerecord_postgres_test/feature/rules/backend/conftest.py
from __future__ import annotations

import importlib

import pytest

from rhosocial.activerecord.backend.expression_registry import ExpressionRegistry
from rhosocial.activerecord.backend.impl.postgres.expression_registry import (
    POSTGRES_EXPRESSION_REGISTRY,
)
from rhosocial.activerecord.testsuite.feature.rules.backend.support import impl_root_of
from rhosocial.activerecord.testsuite.feature.rules.contracts.subject import (
    ExpressionSubject,
    Root,
)

IMPL = "rhosocial.activerecord.backend.impl.postgres"
PREFIX = "Postgres"


@pytest.fixture
def subject() -> ExpressionSubject:
    """postgres's namespace, described without spelling its root."""
    expression = importlib.import_module(f"{IMPL}.expression")
    root = impl_root_of(expression)
    return ExpressionSubject(
        label="postgres",
        roots=(Root("postgres", root, IMPL),),
        prefix=PREFIX,
        open_package_root=root,
        impl_module_prefix=IMPL,
    )


@pytest.fixture
def registry() -> ExpressionRegistry:
    """postgres's published expression table, supplied to the shared contract."""
    return POSTGRES_EXPRESSION_REGISTRY
