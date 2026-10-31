# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_model_truncate.py
"""PostgreSQL rendering tests for model-bound TRUNCATE expressions."""

from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.statements.ddl_truncate import (
    TruncateStatement,
)


TABLE = "model_truncate_events"


class TestSyncModelTruncate:
    def test_truncate_expression_renders_postgres_options(self, postgres_backend):
        expression = TruncateStatement(
            postgres_backend.dialect,
            Table(postgres_backend.dialect, TABLE),
            restart_identity=True,
            cascade=True,
        )

        assert isinstance(expression, TruncateStatement)
        assert expression.table.name == TABLE
        assert expression.restart_identity is True
        assert expression.cascade is True
        assert expression.to_sql() == (
            f'TRUNCATE TABLE "{TABLE}" RESTART IDENTITY CASCADE',
            (),
        )


class TestAsyncModelTruncate:
    async def test_truncate_expression_renders_postgres_options(
        self,
        async_postgres_backend,
    ):
        expression = TruncateStatement(
            async_postgres_backend.dialect,
            Table(async_postgres_backend.dialect, TABLE),
            restart_identity=True,
            cascade=True,
        )

        assert isinstance(expression, TruncateStatement)
        assert expression.table.name == TABLE
        assert expression.restart_identity is True
        assert expression.cascade is True
        assert expression.to_sql() == (
            f'TRUNCATE TABLE "{TABLE}" RESTART IDENTITY CASCADE',
            (),
        )
