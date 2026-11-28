# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/expression/test_expression_registry.py
"""Construct-level guards for PostgreSQL's published expression table.

The table is data (``kind -> fully qualified class name``) merged from core's
kinds and this backend's own; a dialect only renders. Nothing here opens a
connection -- the tests read the published mapping and render a few nodes with
a bare ``PostgresDialect``.
"""

import pytest

from rhosocial.activerecord.backend.expression.core import Column, Parameter
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.expression_registry import CORE_EXPRESSIONS
from rhosocial.activerecord.backend.impl.dummy.expression_registry import DUMMY_EXPRESSIONS
from rhosocial.activerecord.backend.impl.postgres.expression.advisory.lock import (
    PostgresAdvisoryLockExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression_registry import (
    POSTGRES_EXPRESSIONS,
    POSTGRES_EXPRESSION_REGISTRY,
)
from rhosocial.activerecord.backend.impl.sqlite.expression_registry import SQLITE_EXPRESSIONS

_PG_KIND = "postgres_advisory_lock_expression"
_SIBLING_ONLY = (set(SQLITE_EXPRESSIONS) | set(DUMMY_EXPRESSIONS)) - set(CORE_EXPRESSIONS)


def test_registry_is_frozen_and_holds_exactly_core_plus_postgres_kinds():
    registry = POSTGRES_EXPRESSION_REGISTRY
    assert registry.is_frozen
    assert set(registry.node_kinds()) == set(CORE_EXPRESSIONS) | set(POSTGRES_EXPRESSIONS)

    with pytest.raises(Exception) as caught:
        registry.register_node(Column)
    assert "frozen" in str(caught.value)


def test_no_sibling_backend_only_kind_is_published():
    assert _SIBLING_ONLY, "sanity: the sibling tables must contribute at least one kind"
    assert not set(POSTGRES_EXPRESSION_REGISTRY.node_kinds()) & _SIBLING_ONLY


def test_parameter_kind_resolves_to_cores_parameter():
    assert POSTGRES_EXPRESSION_REGISTRY.resolve_node("parameter") is Parameter


def test_a_postgres_owned_kind_resolves_to_its_class():
    assert POSTGRES_EXPRESSIONS[_PG_KIND] == (
        f"{PostgresAdvisoryLockExpression.__module__}."
        f"{PostgresAdvisoryLockExpression.__qualname__}"
    )
    assert POSTGRES_EXPRESSION_REGISTRY.resolve_node(_PG_KIND) is PostgresAdvisoryLockExpression


def test_parameter_binds_by_default(postgres_dialect):
    sql, params = Parameter(postgres_dialect, 42).to_sql()
    assert sql == "%s"
    assert params == (42,)


def test_inline_parameter_renders_backend_sql_text_with_no_params(postgres_dialect):
    sql, params = Parameter(postgres_dialect, "O'Brien", inline=True).to_sql()
    assert sql == "'O''Brien'"
    assert params == ()


def test_raw_value_at_a_value_position_is_bound_not_interpolated(postgres_dialect):
    payload = "x' OR 1=1 --"
    predicate = ComparisonPredicate(
        postgres_dialect, "=", Column(postgres_dialect, "name"), payload
    )
    assert isinstance(predicate.right, Parameter)
    assert predicate.right.value == payload

    sql, params = predicate.to_sql()
    assert sql == '"name" = %s'
    assert params == (payload,)
    assert payload not in sql
