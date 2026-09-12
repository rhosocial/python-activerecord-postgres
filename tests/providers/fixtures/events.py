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
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "name", VarCharType(length=255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "status", VarCharType(length=50, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value="draft")]),
            ColumnDefinition(dialect, "revision", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=1)]),
            ColumnDefinition(dialect, "content", TextType(dialect=dialect)),
            ColumnDefinition(dialect, "created_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "updated_at", TimestampTzType(dialect=dialect)),
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
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "title", VarCharType(length=255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "content", TextType(dialect=dialect)),
            ColumnDefinition(dialect, "view_count", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=0)]),
            ColumnDefinition(dialect, "last_viewed_at", TimestampTzType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NULL)]),
        ],
    )


TABLE_EXPRESSIONS: Dict[str, Callable] = {
    "event_tests": create_event_tests_table,
    "event_tracking_models": create_event_tracking_models_table,
}
