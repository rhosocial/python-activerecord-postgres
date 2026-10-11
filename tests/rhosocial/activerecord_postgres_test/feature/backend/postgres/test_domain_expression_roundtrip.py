# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_domain_expression_roundtrip.py
"""Round-trip contract for ``PostgresCreateDomainExpression``.

``CREATE DOMAIN`` has two mutually exclusive spellings for the same list of
CHECK clauses: ``constraints`` (historical, also accepts the deprecated raw
SQL string form) and ``checks`` (typed). This class used to override
``get_params()`` to re-key that list onto whichever spelling the caller used;
core's contract test
``test_expression_contract.py::TestInitParamAttributeContract::test_no_get_params_override``
forbids that, because the generic introspection path is the single
serialization path.

The repair is to fold the state into ``__init__``: it stores the merged list
under the slot matching the parameter the caller used and leaves the unused
spelling ``None``. These tests prove the generic path then reconstructs the
same expression — from *both* spellings — with identical SQL, and that it
never emits a key the constructor would reject.
"""

import inspect
import warnings

import pytest

from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.serialization import (
    deserialize,
    deserialize_json,
    serialize,
    serialize_json,
)
from rhosocial.activerecord.backend.expression.statements.ddl_domain import (
    DomainCheckConstraint,
    DomainNullability,
    DomainValueExpression,
)
from rhosocial.activerecord.backend.expression.types import IntegerType
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.domain import (
    PostgresCreateDomainExpression,
)
from rhosocial.activerecord.testsuite.utils.expression import assert_params_equal


@pytest.fixture
def dialect():
    return PostgresDialect(version=(14, 0, 0))


def _condition(dialect):
    return DomainValueExpression(dialect) > Literal(dialect, 0, inline_literals=True)


def _check(dialect, name=None):
    return DomainCheckConstraint(dialect, _condition(dialect), name=name)


def _round_trip(expr, dialect):
    """get_params() -> reconstruct from those params alone -> prove fidelity.

    Returns the emitted params so the caller can inspect their shape.
    """
    params = expr.get_params()

    accepted = set(inspect.signature(type(expr).__init__).parameters)
    rejected = sorted((set(params) | set(serialize(expr)["params"])) - accepted)
    assert not rejected, (
        f"get_params() emitted keys the constructor would reject: {rejected}"
    )

    for restored in (
        deserialize(serialize(expr), dialect),
        deserialize_json(serialize_json(expr), dialect),
    ):
        rebuilt = type(expr)(dialect, **restored.get_params())
        assert_params_equal(rebuilt.get_params(), expr.get_params())
        assert rebuilt.to_sql() == expr.to_sql()

    return params


class TestPostgresCreateDomainRoundTrip:
    """Both spellings survive the generic get_params() round trip."""

    def test_constraints_spelling_round_trips(self, dialect):
        check = _check(dialect, name="positive")
        expr = PostgresCreateDomainExpression(
            dialect,
            "amount",
            IntegerType(dialect),
            schema="app",
            collation="public.catalog",
            default=0,
            constraints=[check],
            nullability=DomainNullability.NOT_NULL,
        )

        params = _round_trip(expr, dialect)

        # The used spelling carries the list; the unused one says "not supplied".
        assert params["constraints"] == [check]
        assert params["checks"] is None
        assert expr.constraints == [check]
        assert expr.checks is None
        assert expr.check_constraints == [check]
        assert expr.to_sql()[0] == (
            'CREATE DOMAIN "app"."amount" AS INTEGER COLLATE "public"."catalog" '
            'DEFAULT 0 CONSTRAINT "positive" CHECK (VALUE > 0) NOT NULL'
        )

    def test_checks_spelling_round_trips(self, dialect):
        condition = _condition(dialect)
        expr = PostgresCreateDomainExpression(
            dialect,
            "amount",
            IntegerType(dialect),
            schema="app",
            collation="public.catalog",
            default=0,
            checks=[condition],
            nullability=DomainNullability.NOT_NULL,
        )

        params = _round_trip(expr, dialect)

        # A bare SQLPredicate is normalised into a DomainCheckConstraint by the
        # base class; that normalised list is what the `checks` slot keeps.
        assert [type(c) for c in params["checks"]] == [DomainCheckConstraint]
        assert params["checks"] == expr.check_constraints
        assert params["constraints"] is None
        assert expr.constraints is None
        assert expr.to_sql()[0] == (
            'CREATE DOMAIN "app"."amount" AS INTEGER COLLATE "public"."catalog" '
            'DEFAULT 0 CHECK (VALUE > 0) NOT NULL'
        )

    def test_checks_spelling_with_named_check_round_trips(self, dialect):
        check = _check(dialect, name="positive")
        expr = PostgresCreateDomainExpression(
            dialect, "amount", IntegerType(dialect), checks=[check]
        )

        params = _round_trip(expr, dialect)

        assert params["checks"] == [check]
        assert params["constraints"] is None
        assert expr.to_sql()[0] == (
            'CREATE DOMAIN "amount" AS INTEGER CONSTRAINT "positive" CHECK (VALUE > 0)'
        )

    def test_neither_spelling_round_trips(self, dialect):
        expr = PostgresCreateDomainExpression(dialect, "bare", IntegerType(dialect))

        params = _round_trip(expr, dialect)

        assert params["constraints"] is None
        assert params["checks"] is None
        assert expr.to_sql()[0] == 'CREATE DOMAIN "bare" AS INTEGER'

    def test_legacy_raw_constraint_string_round_trips(self, dialect):
        expr = PostgresCreateDomainExpression(
            dialect,
            "legacy",
            IntegerType(dialect),
            default=0,
            constraints=["CHECK (VALUE > 0)"],
        )

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            params = _round_trip(expr, dialect)

        assert params["constraints"] == ["CHECK (VALUE > 0)"]
        assert params["checks"] is None
        assert expr.to_sql()[0] == (
            'CREATE DOMAIN "legacy" AS INTEGER DEFAULT 0 CHECK (VALUE > 0)'
        )

    def test_both_spellings_at_once_still_rejected(self, dialect):
        with pytest.raises(ValueError, match="mutually exclusive"):
            PostgresCreateDomainExpression(
                dialect,
                "both",
                IntegerType(dialect),
                constraints=[_check(dialect)],
                checks=[_check(dialect)],
            )

    def test_class_does_not_override_get_params(self):
        from rhosocial.activerecord.backend.expression.bases import BaseExpression

        assert PostgresCreateDomainExpression.get_params is BaseExpression.get_params