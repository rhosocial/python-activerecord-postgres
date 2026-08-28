# tests/rhosocial/activerecord_postgres_test/feature/backend/test_expression_roundtrip_all.py
"""
Functional serialization coverage for PostgreSQL expression classes.

Every expression class defined in ``rhosocial.activerecord.backend.impl
.postgres.expression`` must round-trip losslessly through dict / JSON / XML
encodings, and produce identical ``to_sql()`` where the PostgreSQL dialect
supports it.
"""

import pytest

from rhosocial.activerecord.testsuite.utils.expression import (
    collect_expression_classes,
    make_instance,
    register_all,
    register_special_constructor,
    roundtrip_expression,
    sql_consistent,
)

PG_EXPR_PKG = "rhosocial.activerecord.backend.impl.postgres.expression"

CLASSES = collect_expression_classes(PG_EXPR_PKG)
register_all(CLASSES)


def _register_postgres_specials():
    from rhosocial.activerecord.backend.expression.core import Column, Literal

    def for_update(d):
        from rhosocial.activerecord.backend.impl.postgres.expression.locking import (
            PostgresForUpdateExpression,
        )
        return PostgresForUpdateExpression(d, columns=[Column(d, "id")])

    def advisory(d):
        from rhosocial.activerecord.backend.impl.postgres.expression.advisory.lock import (
            PostgresAdvisoryLockExpression,
        )
        return PostgresAdvisoryLockExpression(d, key=1)

    register_special_constructor("locking.PostgresForUpdateExpression", for_update)
    register_special_constructor("advisory.lock.PostgresAdvisoryLockExpression", advisory)


_register_postgres_specials()


@pytest.fixture(params=[fqn for fqn in sorted(CLASSES)], ids=sorted(CLASSES))
def postgres_expr_case(request, postgres_dialect):
    fqn = request.param
    cls = CLASSES[fqn]
    instance, source = make_instance(cls, postgres_dialect)
    if instance is None:
        pytest.skip(f"{fqn}: {source}")
    return fqn, instance


class TestPostgresExpressionRoundtrip:
    """All constructible PostgreSQL expression classes round-trip losslessly."""

    def test_get_params_roundtrip(self, postgres_expr_case, postgres_dialect):
        fqn, instance = postgres_expr_case
        roundtrip_expression(fqn, instance, postgres_dialect)

    def test_to_sql_consistent(self, postgres_expr_case, postgres_dialect):
        fqn, instance = postgres_expr_case
        sql_consistent(fqn, instance, postgres_dialect)


def test_core_expressions_also_roundtrip(postgres_dialect):
    from rhosocial.activerecord.backend.expression.core import Column, Literal
    from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate

    expr = ComparisonPredicate(
        postgres_dialect, "=", Column(postgres_dialect, "a"), Literal(postgres_dialect, 1)
    )
    roundtrip_expression("core", expr, postgres_dialect)
    sql_consistent("core", expr, postgres_dialect)