# tests/rhosocial/activerecord_postgres_test/feature/backend/test_master_probe_execution.py
"""Execution confirmation for the master probes this backend declares.

Rendering is not execution: the probe answers below are confirmed against the
live server the scenario carries. Every verdict is preceded by a sentinel --
a deliberately invalid statement must classify ``REJECTED`` -- and an accepted
control, so a null result can never be read as acceptance.

The declarations were measured on this repository's matrix (PostgreSQL 9.6
through 19beta4); this file keeps that measurement from rotting, and each test
asserts outcome against the dialect's own probe answer so a probe that drifts
from the server fails here rather than in the field.
"""

import pytest

from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.execution_testing import (
    ExecutionOutcome,
    classify_execution,
    confirm_expression_execution,
)
from rhosocial.activerecord.backend.expression.objects import (
    MaterializedView,
    Table,
)
from rhosocial.activerecord.backend.expression.query_sources import (
    CTEExpression,
    WithQueryExpression,
)
from rhosocial.activerecord.backend.expression.statements.ddl_table import (
    CreateTableAsExpression,
    DropTableExpression,
)
from rhosocial.activerecord.backend.expression.statements.ddl_truncate import (
    TruncateExpression,
)
from rhosocial.activerecord.backend.expression.statements.ddl_view import (
    CreateMaterializedViewExpression,
    DropMaterializedViewExpression,
    RefreshMaterializedViewExpression,
)
from rhosocial.activerecord.backend.expression.statements.dql import QueryExpression

from rhosocial.activerecord.backend.impl.postgres.expression.ddl.mv import (
    PostgresRefreshMaterializedViewExpression,
)


def _table(d, name):
    return Table(d, name)


def _literal(d, value=1):
    """An inlined literal: MV/CTAS bodies reject bind parameters on PostgreSQL."""
    return Literal(d, value, inline_literals=True)


def _query(d):
    return QueryExpression(d, select=[_literal(d)])


def _cte_query(d, cte_name, **kwargs):
    """A WITH query whose main query selects from the single CTE it defines."""
    return WithQueryExpression(
        d,
        ctes=[
            CTEExpression(
                d,
                cte_name,
                QueryExpression(d, select=[_literal(d)]),
                **kwargs,
            )
        ],
        main_query=QueryExpression(
            d, select=[_literal(d)], from_=_table(d, cte_name)
        ),
    )


class TestMasterProbeExecutionConfirmation:
    """The four master probes' outcomes are confirmed per live server."""

    def test_sentinel_rejected_and_control_accepted(self, postgres_backend):
        """The classifier must see both answers before any verdict is trusted."""
        assert (
            classify_execution(postgres_backend, "THIS IS NOT SQL")
            is ExecutionOutcome.REJECTED
        )
        assert (
            classify_execution(postgres_backend, "SELECT 1")
            is ExecutionOutcome.ACCEPTED
        )

    @pytest.mark.parametrize(
        "side,kwargs,table_name",
        [
            ("with_data", {"with_data": True}, "master_probe_ctas_with"),
            ("no_data", {"no_data": True}, "master_probe_ctas_no"),
        ],
        ids=["with_data", "no_data"],
    )
    def test_ctas_with_data_matches_the_probe(
        self, postgres_backend, side, kwargs, table_name
    ):
        dialect = postgres_backend.dialect
        outcome = confirm_expression_execution(
            postgres_backend,
            CreateTableAsExpression(
                dialect, _table(dialect, table_name), _query(dialect), **kwargs
            ),
            teardown=[
                DropTableExpression(
                    dialect, _table(dialect, table_name), if_exists=True
                )
            ],
        )
        if dialect.supports_with_data_clause():
            assert outcome is ExecutionOutcome.ACCEPTED
        else:
            assert outcome is ExecutionOutcome.NOT_RENDERED

    @pytest.mark.parametrize(
        "side,kwargs,view_name",
        [
            ("with_data", {"with_data": True}, "master_probe_mv_with"),
            ("no_data", {"no_data": True}, "master_probe_mv_no"),
        ],
        ids=["with_data", "no_data"],
    )
    def test_create_materialized_view_with_data_matches_the_probe(
        self, postgres_backend, side, kwargs, view_name
    ):
        dialect = postgres_backend.dialect
        view = MaterializedView(dialect, view_name)
        outcome = confirm_expression_execution(
            postgres_backend,
            CreateMaterializedViewExpression(dialect, view, _query(dialect), **kwargs),
            teardown=[
                DropMaterializedViewExpression(dialect, view, if_exists=True)
            ],
        )
        if dialect.supports_materialized_view() and dialect.supports_with_data_clause():
            assert outcome is ExecutionOutcome.ACCEPTED
        else:
            assert outcome is ExecutionOutcome.NOT_RENDERED

    @pytest.mark.parametrize(
        "expression_cls,side,kwargs",
        [
            (RefreshMaterializedViewExpression, "with_data", {"with_data": True}),
            (RefreshMaterializedViewExpression, "no_data", {"no_data": True}),
            (
                PostgresRefreshMaterializedViewExpression,
                "with_data",
                {"with_data": True},
            ),
            (
                PostgresRefreshMaterializedViewExpression,
                "no_data",
                {"no_data": True},
            ),
        ],
        ids=[
            "generic-with_data",
            "generic-no_data",
            "postgres-with_data",
            "postgres-no_data",
        ],
    )
    def test_refresh_materialized_view_with_data_matches_the_probe(
        self, postgres_backend, expression_cls, side, kwargs
    ):
        dialect = postgres_backend.dialect
        view_name = "master_probe_mv_refresh"
        view = MaterializedView(dialect, view_name)
        teardown = [
            DropMaterializedViewExpression(dialect, view, if_exists=True)
        ]

        if dialect.supports_materialized_view():
            # Build an unpopulated MV so the refresh has something to target.
            prepare = [
                CreateMaterializedViewExpression(
                    dialect, view, _query(dialect), no_data=True
                )
            ]
        else:
            prepare = []

        outcome = confirm_expression_execution(
            postgres_backend,
            expression_cls(dialect, view, **kwargs),
            prepare=prepare,
            teardown=teardown,
        )
        if (
            dialect.supports_refresh_materialized_view()
            and dialect.supports_with_data_clause()
        ):
            assert outcome is ExecutionOutcome.ACCEPTED
        else:
            assert outcome is ExecutionOutcome.NOT_RENDERED

    @pytest.mark.parametrize(
        "side,kwargs",
        [
            ("materialized", {"materialized": True}),
            ("not_materialized", {"not_materialized": True}),
        ],
        ids=["materialized", "not_materialized"],
    )
    def test_materialized_cte_matches_the_probe(self, postgres_backend, side, kwargs):
        dialect = postgres_backend.dialect
        outcome = confirm_expression_execution(
            postgres_backend,
            _cte_query(dialect, f"master_probe_cte_{side}", **kwargs),
        )
        if dialect.supports_materialized_cte():
            assert outcome is ExecutionOutcome.ACCEPTED
        else:
            # PostgreSQL 11 and older: the hint is a syntax error, so the
            # expression must refuse instead of rendering it.
            assert outcome is ExecutionOutcome.NOT_RENDERED

    def test_truncate_matches_the_probe(self, postgres_backend):
        dialect = postgres_backend.dialect
        table_name = "master_probe_truncate"
        outcome = confirm_expression_execution(
            postgres_backend,
            TruncateExpression(dialect, _table(dialect, table_name)),
            prepare=[
                CreateTableAsExpression(
                    dialect, _table(dialect, table_name), _query(dialect)
                )
            ],
            teardown=[
                DropTableExpression(
                    dialect, _table(dialect, table_name), if_exists=True
                )
            ],
        )
        if dialect.supports_truncate():
            assert outcome is ExecutionOutcome.ACCEPTED
        else:
            assert outcome is ExecutionOutcome.NOT_RENDERED

    def test_server_rejects_wait_spelling(self, postgres_backend):
        """WAIT / NO WAIT is not PostgreSQL grammar; the False probe holds."""
        assert (
            classify_execution(postgres_backend, "BEGIN NO WAIT")
            is ExecutionOutcome.REJECTED
        )
        assert (
            classify_execution(postgres_backend, "SET TRANSACTION NO WAIT")
            is ExecutionOutcome.REJECTED
        )
