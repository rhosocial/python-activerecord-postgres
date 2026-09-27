# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_model_truncate.py
"""PostgreSQL rendering tests for model-bound TRUNCATE expressions."""

from rhosocial.activerecord.backend.expression.statements.ddl_truncate import (
    TruncateExpression,
)


TABLE = "model_truncate_events"


class TestSyncModelTruncate:
    def test_truncate_expression_renders_postgres_options(self, postgres_backend):
        expression = TruncateExpression(
            postgres_backend.dialect,
            TABLE,
            restart_identity=True,
            cascade=True,
        )

        assert isinstance(expression, TruncateExpression)
        assert expression.table_name == TABLE
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
        expression = TruncateExpression(
            async_postgres_backend.dialect,
            TABLE,
            restart_identity=True,
            cascade=True,
        )

        assert isinstance(expression, TruncateExpression)
        assert expression.table_name == TABLE
        assert expression.restart_identity is True
        assert expression.cascade is True
        assert expression.to_sql() == (
            f'TRUNCATE TABLE "{TABLE}" RESTART IDENTITY CASCADE',
            (),
        )
