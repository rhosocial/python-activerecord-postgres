# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/expression/test_expression_registry.py
"""Construct-level guards for PostgreSQL parameter rendering.

The registry-table contracts live in the shared suite; what remains here is
backend-specific rendering: binding by default, inline SQL text, and coercing a
raw value at a value position into a bound parameter. Nothing here opens a
connection -- rendering uses a bare ``PostgresDialect``.
"""

from rhosocial.activerecord.backend.expression.column_types.column import Column
from rhosocial.activerecord.backend.expression.value_types.parameter import Parameter
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate


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
