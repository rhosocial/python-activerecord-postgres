# tests/providers/fixtures/events.py
"""DDL expressions for the ``feature/events`` table group (PostgreSQL).

Reference: ``tests/rhosocial/activerecord_postgres_test/feature/events/schema/``.
"""

from typing import Callable, Dict

from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
)
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)
from rhosocial.activerecord.backend.expression.types import (
    IntegerType,
    TextType,
    TimestampTzType,
    VarCharType,
)
from rhosocial.activerecord.backend.impl.postgres import (
    PostgresSerialType,
)


# ---------------------------------------------------------------------------
# events/event_tests.sql
# ---------------------------------------------------------------------------

def create_event_tests_table(dialect, table_name: str = "event_tests") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("name", VarCharType(255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("status", VarCharType(50),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value="draft")]),
            ColumnDefinition("revision", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=1)]),
            ColumnDefinition("content", TextType()),
            ColumnDefinition("created_at", TimestampTzType()),
            ColumnDefinition("updated_at", TimestampTzType()),
        ],
    )


# ---------------------------------------------------------------------------
# events/event_tracking_models.sql
# ---------------------------------------------------------------------------

def create_event_tracking_models_table(dialect, table_name: str = "event_tracking_models") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("title", VarCharType(255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("content", TextType()),
            ColumnDefinition("view_count", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=0)]),
            ColumnDefinition("last_viewed_at", TimestampTzType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NULL)]),
        ],
    )


TABLE_EXPRESSIONS: Dict[str, Callable] = {
    "event_tests": create_event_tests_table,
    "event_tracking_models": create_event_tracking_models_table,
}
